import express from 'express';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { Connection, PublicKey, Keypair } from '@solana/web3.js';
import { Program, AnchorProvider, BN } from '@project-serum/anchor';

// Load environment variables
dotenv.config();

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
);

// Initialize Solana connection
const connection = new Connection(process.env.SOLANA_RPC_URL, 'confirmed');

// Initialize Anchor program (we'll need to import the IDL)
const WAGERFI_PROGRAM_ID = new PublicKey(process.env.WAGERFI_PROGRAM_ID);

// Authority keypair for executing program instructions
// This should be loaded from environment or secure storage
const AUTHORITY_PRIVATE_KEY = process.env.AUTHORITY_PRIVATE_KEY;
const authorityKeypair = AUTHORITY_PRIVATE_KEY ?
    Keypair.fromSecretKey(Buffer.from(JSON.parse(AUTHORITY_PRIVATE_KEY))) :
    null;

if (!authorityKeypair) {
    console.error('❌ AUTHORITY_PRIVATE_KEY not found. Cannot execute on-chain transactions.');
    process.exit(1);
}

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        service: 'wagerfi-bgworker',
        version: '1.0.0',
        authority: authorityKeypair.publicKey.toString()
    });
});

// Status endpoint
app.get('/status', (req, res) => {
    res.json({
        service: 'WagerFi Background Worker',
        environment: process.env.NODE_ENV || 'development',
        authority: authorityKeypair.publicKey.toString(),
        timestamp: new Date().toISOString()
    });
});

// IMMEDIATE EXECUTION FUNCTIONS (no cron jobs)

// 1. Resolve Crypto Wager (always has a winner)
app.post('/resolve-crypto-wager', async (req, res) => {
    try {
        const { wager_id } = req.body;

        if (!wager_id) {
            return res.status(400).json({ error: 'wager_id is required' });
        }

        console.log(`🔄 Resolving crypto wager: ${wager_id}`);

        // Get wager from database
        const { data: wager, error: fetchError } = await supabase
            .from('crypto_wagers')
            .select('*')
            .eq('wager_id', wager_id)
            .eq('status', 'active')
            .single();

        if (fetchError || !wager) {
            return res.status(404).json({ error: 'Wager not found or not active' });
        }

        // Get current price from CoinMarketCap API
        const currentPrice = await getCurrentCryptoPrice(wager.token_symbol);

        // Determine winner based on prediction
        let winnerId = null;
        let resolutionPrice = currentPrice;

        if (wager.prediction_type === 'above') {
            winnerId = currentPrice > wager.target_price ? wager.creator_id : wager.acceptor_id;
        } else {
            winnerId = currentPrice < wager.target_price ? wager.creator_id : wager.acceptor_id;
        }

        // Execute on-chain resolution using your token program
        const resolutionResult = await resolveCryptoWagerOnChain(
            wager_id,
            winnerId,
            wager.creator_id,
            wager.acceptor_id,
            wager.amount
        );

        if (!resolutionResult.success) {
            return res.status(500).json({ error: resolutionResult.error });
        }

        // Update database with resolution
        const { error: updateError } = await supabase
            .from('crypto_wagers')
            .update({
                status: 'resolved',
                winner_id: winnerId,
                resolution_price: resolutionPrice,
                resolution_time: new Date().toISOString(),
                on_chain_signature: resolutionResult.signature
            })
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating wager ${wager_id}:`, updateError);
            return res.status(500).json({ error: 'Failed to update database' });
        }

        // Create notification for winner
        await createNotification(winnerId, 'wager_resolved',
            'Wager Resolved!',
            `Your crypto wager on ${wager.token_symbol} has been resolved. You won ${wager.amount} SOL!`);

        // Update user stats
        await updateUserStats(winnerId);

        console.log(`✅ Resolved crypto wager ${wager_id} - Winner: ${winnerId}`);

        res.json({
            success: true,
            wager_id,
            winner_id: winnerId,
            resolution_price: resolutionPrice,
            on_chain_signature: resolutionResult.signature
        });

    } catch (error) {
        console.error('❌ Error resolving crypto wager:', error);
        res.status(500).json({ error: error.message });
    }
});

// 2. Resolve Sports Wager (can have draws)
app.post('/resolve-sports-wager', async (req, res) => {
    try {
        const { wager_id } = req.body;

        if (!wager_id) {
            return res.status(400).json({ error: 'wager_id is required' });
        }

        console.log(`🔄 Resolving sports wager: ${wager_id}`);

        // Get wager from database
        const { data: wager, error: fetchError } = await supabase
            .from('sports_wagers')
            .select('*')
            .eq('wager_id', wager_id)
            .eq('status', 'active')
            .single();

        if (fetchError || !wager) {
            return res.status(404).json({ error: 'Wager not found or not active' });
        }

        // Get game result from Sports API
        const gameResult = await getSportsGameResult(wager.sport, wager.team1, wager.team2);

        let winnerId = null;
        let resolutionOutcome = gameResult;
        let isDraw = false;

        // Check if it's a draw
        if (gameResult === 'draw' || gameResult === 'tie') {
            isDraw = true;
            // For draws, we need to handle differently - both parties get refunded
        } else if (gameResult === wager.prediction) {
            winnerId = wager.creator_id;
        } else {
            winnerId = wager.acceptor_id;
        }

        let onChainResult;

        if (isDraw) {
            // Handle draw - refund both parties
            onChainResult = await handleSportsDrawOnChain(
                wager_id,
                wager.creator_id,
                wager.acceptor_id,
                wager.amount
            );
        } else {
            // Handle normal resolution
            onChainResult = await resolveSportsWagerOnChain(
                wager_id,
                winnerId,
                wager.creator_id,
                wager.acceptor_id,
                wager.amount
            );
        }

        if (!onChainResult.success) {
            return res.status(500).json({ error: onChainResult.error });
        }

        // Update database
        const { error: updateError } = await supabase
            .from('sports_wagers')
            .update({
                status: 'resolved',
                winner_id: winnerId,
                resolution_outcome: resolutionOutcome,
                resolution_time: new Date().toISOString(),
                on_chain_signature: onChainResult.signature
            })
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating wager ${wager_id}:`, updateError);
            return res.status(500).json({ error: 'Failed to update database' });
        }

        // Create notifications
        if (isDraw) {
            await createNotification(wager.creator_id, 'wager_resolved',
                'Wager Draw!',
                `Your sports wager on ${wager.team1} vs ${wager.team2} ended in a draw. You've been refunded ${wager.amount} SOL.`);
            await createNotification(wager.acceptor_id, 'wager_resolved',
                'Wager Draw!',
                `Your sports wager on ${wager.team1} vs ${wager.team2} ended in a draw. You've been refunded ${wager.amount} SOL.`);
        } else {
            await createNotification(winnerId, 'wager_resolved',
                'Wager Resolved!',
                `Your sports wager on ${wager.team1} vs ${wager.team2} has been resolved. You won ${wager.amount} SOL!`);
        }

        // Update user stats
        if (winnerId) {
            await updateUserStats(winnerId);
        }

        console.log(`✅ Resolved sports wager ${wager_id} - ${isDraw ? 'Draw' : `Winner: ${winnerId}`}`);

        res.json({
            success: true,
            wager_id,
            winner_id: winnerId,
            resolution_outcome: resolutionOutcome,
            is_draw: isDraw,
            on_chain_signature: onChainResult.signature
        });

    } catch (error) {
        console.error('❌ Error resolving sports wager:', error);
        res.status(500).json({ error: error.message });
    }
});

// 3. Cancel Wager (refund creator)
app.post('/cancel-wager', async (req, res) => {
    try {
        const { wager_id, wager_type } = req.body;

        if (!wager_id || !wager_type) {
            return res.status(400).json({ error: 'wager_id and wager_type are required' });
        }

        console.log(`🔄 Cancelling ${wager_type} wager: ${wager_id}`);

        // Get wager from database
        const tableName = wager_type === 'crypto' ? 'crypto_wagers' : 'sports_wagers';
        const { data: wager, error: fetchError } = await supabase
            .from(tableName)
            .select('*')
            .eq('wager_id', wager_id)
            .eq('status', 'open')
            .single();

        if (fetchError || !wager) {
            return res.status(404).json({ error: 'Wager not found or not open' });
        }

        // Execute on-chain cancellation and refund
        const cancellationResult = await cancelWagerOnChain(
            wager_id,
            wager.creator_id,
            wager.amount
        );

        if (!cancellationResult.success) {
            return res.status(500).json({ error: cancellationResult.error });
        }

        // Update database
        const { error: updateError } = await supabase
            .from(tableName)
            .update({
                status: 'cancelled',
                on_chain_signature: cancellationResult.signature
            })
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating wager ${wager_id}:`, updateError);
            return res.status(500).json({ error: 'Failed to update database' });
        }

        // Create notification for creator
        await createNotification(wager.creator_id, 'wager_cancelled',
            'Wager Cancelled!',
            `Your ${wager_type} wager has been cancelled and you've been refunded ${wager.amount} SOL.`);

        console.log(`✅ Cancelled ${wager_type} wager ${wager_id}`);

        res.json({
            success: true,
            wager_id,
            wager_type,
            on_chain_signature: cancellationResult.signature
        });

    } catch (error) {
        console.error('❌ Error cancelling wager:', error);
        res.status(500).json({ error: error.message });
    }
});

// 4. Handle Expired Wager (automatic refund)
app.post('/handle-expired-wager', async (req, res) => {
    try {
        const { wager_id, wager_type } = req.body;

        if (!wager_id || !wager_type) {
            return res.status(400).json({ error: 'wager_id and wager_type are required' });
        }

        console.log(`🔄 Handling expired ${wager_type} wager: ${wager_id}`);

        // Get wager from database
        const tableName = wager_type === 'crypto' ? 'crypto_wagers' : 'sports_wagers';
        const { data: wager, error: fetchError } = await supabase
            .from(tableName)
            .select('*')
            .eq('wager_id', wager_id)
            .eq('status', 'active')
            .lt('expiry_time', new Date().toISOString())
            .single();

        if (fetchError || !wager) {
            return res.status(404).json({ error: 'Wager not found, not active, or not expired' });
        }

        // Execute on-chain expiration handling (refund creator)
        const expirationResult = await handleExpiredWagerOnChain(
            wager_id,
            wager.creator_id,
            wager.amount
        );

        if (!expirationResult.success) {
            return res.status(500).json({ error: expirationResult.error });
        }

        // Update database
        const { error: updateError } = await supabase
            .from(tableName)
            .update({
                status: 'expired',
                on_chain_signature: expirationResult.signature
            })
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating wager ${wager_id}:`, updateError);
            return res.status(500).json({ error: 'Failed to update database' });
        }

        // Create notification for creator
        await createNotification(wager.creator_id, 'wager_expired',
            'Wager Expired!',
            `Your ${wager_type} wager has expired and you've been refunded ${wager.amount} SOL.`);

        console.log(`✅ Handled expired ${wager_type} wager ${wager_id}`);

        res.json({
            success: true,
            wager_id,
            wager_type,
            on_chain_signature: expirationResult.signature
        });

    } catch (error) {
        console.error('❌ Error handling expired wager:', error);
        res.status(500).json({ error: error.message });
    }
});

// ON-CHAIN INTEGRATION FUNCTIONS

// Resolve crypto wager on-chain
async function resolveCryptoWagerOnChain(wagerId, winnerId, creatorId, acceptorId, amount) {
    try {
        // This would call your token program's resolveWager instruction
        // For now, returning mock success
        console.log(`🔗 Executing on-chain resolution for wager ${wagerId}`);

        // TODO: Implement actual Solana transaction
        // const transaction = await program.methods.resolveWager(winnerId).accounts({...}).rpc();

        return {
            success: true,
            signature: 'mock_signature_' + Date.now()
        };
    } catch (error) {
        console.error('❌ On-chain resolution failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Resolve sports wager on-chain
async function resolveSportsWagerOnChain(wagerId, winnerId, creatorId, acceptorId, amount) {
    try {
        console.log(`🔗 Executing on-chain sports resolution for wager ${wagerId}`);

        // TODO: Implement actual Solana transaction
        // const transaction = await program.methods.resolveWager(winnerId).accounts({...}).rpc();

        return {
            success: true,
            signature: 'mock_signature_' + Date.now()
        };
    } catch (error) {
        console.error('❌ On-chain sports resolution failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Handle sports draw on-chain (refund both parties)
async function handleSportsDrawOnChain(wagerId, creatorId, acceptorId, amount) {
    try {
        console.log(`🔗 Executing on-chain draw handling for wager ${wagerId}`);

        // TODO: Implement actual Solana transaction for draw
        // This would refund both parties their amounts

        return {
            success: true,
            signature: 'mock_draw_signature_' + Date.now()
        };
    } catch (error) {
        console.error('❌ On-chain draw handling failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Cancel wager on-chain (refund creator)
async function cancelWagerOnChain(wagerId, creatorId, amount) {
    try {
        console.log(`🔗 Executing on-chain cancellation for wager ${wagerId}`);

        // TODO: Implement actual Solana transaction
        // const transaction = await program.methods.cancelWager().accounts({...}).rpc();

        return {
            success: true,
            signature: 'mock_cancel_signature_' + Date.now()
        };
    } catch (error) {
        console.error('❌ On-chain cancellation failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Handle expired wager on-chain (refund creator)
async function handleExpiredWagerOnChain(wagerId, creatorId, amount) {
    try {
        console.log(`🔗 Executing on-chain expiration handling for wager ${wagerId}`);

        // TODO: Implement actual Solana transaction
        // const transaction = await program.methods.handleExpiredWager().accounts({...}).rpc();

        return {
            success: true,
            signature: 'mock_expire_signature_' + Date.now()
        };
    } catch (error) {
        console.error('❌ On-chain expiration handling failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// HELPER FUNCTIONS

// Get current crypto price from CoinMarketCap API
async function getCurrentCryptoPrice(symbol) {
    try {
        // TODO: Implement actual CoinMarketCap API call
        // const response = await fetch(`https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=${symbol}`, {
        //     headers: { 'X-CMC_PRO_API_KEY': process.env.COINMARKETCAP_API_KEY }
        // });
        // const data = await response.json();
        // return data.data[symbol].quote.USD.price;

        // Mock prices for now
        const mockPrices = {
            'BTC': 45000,
            'ETH': 3000,
            'SOL': 100
        };
        return mockPrices[symbol] || 100;
    } catch (error) {
        console.error('❌ Error fetching crypto price:', error);
        throw new Error(`Failed to fetch price for ${symbol}`);
    }
}

// Get sports game result from Sports API
async function getSportsGameResult(sport, team1, team2) {
    try {
        // TODO: Implement actual Sports API call
        // This would integrate with your existing sports API

        // Mock result for now
        const results = [team1, team2, 'draw'];
        return results[Math.floor(Math.random() * results.length)];
    } catch (error) {
        console.error('❌ Error fetching sports result:', error);
        throw new Error(`Failed to fetch result for ${team1} vs ${team2}`);
    }
}

// Create notification
async function createNotification(userId, type, title, message) {
    try {
        const { error } = await supabase
            .from('notifications')
            .insert({
                user_id: userId,
                type: type,
                title: title,
                message: message
            });

        if (error) {
            console.error('❌ Error creating notification:', error);
        }
    } catch (error) {
        console.error('❌ Error creating notification:', error);
    }
}

// Update user stats
async function updateUserStats(userId) {
    try {
        const { error } = await supabase.rpc('update_user_stats', { user_uuid: userId });
        if (error) {
            console.error('❌ Error updating user stats:', error);
        }
    } catch (error) {
        console.error('❌ Error updating user stats:', error);
    }
}

// Start the server
app.listen(PORT, () => {
    console.log(`🚀 WagerFi Background Worker running on port ${PORT}`);
    console.log(`📍 Health check: http://localhost:${PORT}/health`);
    console.log(`📊 Status: http://localhost:${PORT}/status`);
    console.log(`🔑 Authority: ${authorityKeypair.publicKey.toString()}`);
    console.log(`⚡ Ready for immediate execution - no cron jobs!`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 Received SIGTERM, shutting down gracefully...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('🛑 Received SIGINT, shutting down gracefully...');
    process.exit(0);
});
