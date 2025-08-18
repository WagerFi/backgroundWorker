import express from 'express';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { PublicKey, Keypair } from '@solana/web3.js';
// import { Program, AnchorProvider } from '@project-serum/anchor';
import fetch from 'node-fetch';

// Load environment variables
dotenv.config();

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
);

// Initialize Solana connection (commented out until needed)
// const connection = new Connection(process.env.SOLANA_RPC_URL, 'confirmed');

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

// Treasury wallet for platform fees (4% total - 2% from each user)
const TREASURY_WALLET = new PublicKey(process.env.TREASURY_WALLET_ADDRESS);

// Platform fee configuration
const PLATFORM_FEE_PERCENTAGE = 0.04; // 4% total (2% from each user)
const SOLANA_TRANSACTION_FEE = 0.000005; // Approximate Solana transaction fee

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        service: 'wagerfi-background-worker',
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
// 
// RESOLUTION STRATEGY:
// - Crypto wagers: Resolve automatically when called (e.g., on expiry)
// - Sports wagers: Check every 5 minutes until result found, then resolve
// - Both: Map winners to positions (creator/acceptor) for token program compatibility

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
        // IMPORTANT: We need to map the winner to their position (creator/acceptor)
        // because the token program expects WinnerType enum, not user IDs
        let winnerId = null;
        let winnerPosition = null; // 'creator' or 'acceptor'
        let resolutionPrice = currentPrice;

        if (wager.prediction_type === 'above') {
            if (currentPrice > wager.target_price) {
                winnerId = wager.creator_id;
                winnerPosition = 'creator';
            } else {
                winnerId = wager.acceptor_id;
                winnerPosition = 'acceptor';
            }
        } else {
            if (currentPrice < wager.target_price) {
                winnerId = wager.creator_id;
                winnerPosition = 'creator';
            } else {
                winnerId = wager.acceptor_id;
                winnerPosition = 'acceptor';
            }
        }

        // Execute on-chain resolution using your token program
        const resolutionResult = await resolveCryptoWagerOnChain(
            wager_id,
            winnerPosition, // Pass 'creator' or 'acceptor' instead of user ID
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
                winner_position: winnerPosition, // Store position for token program compatibility
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

        // IMPORTANT: We need to map the winner to their position (creator/acceptor)
        // because the token program expects WinnerType enum, not user IDs
        let winnerId = null;
        let winnerPosition = null; // 'creator' or 'acceptor'
        let resolutionOutcome = gameResult;
        let isDraw = false;

        // Check if it's a draw
        if (gameResult === 'draw' || gameResult === 'tie') {
            isDraw = true;
            // For draws, we need to handle differently - both parties get refunded
        } else if (gameResult === wager.prediction) {
            winnerId = wager.creator_id;
            winnerPosition = 'creator';
        } else {
            winnerId = wager.acceptor_id;
            winnerPosition = 'acceptor';
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
                winnerPosition, // Pass 'creator' or 'acceptor' instead of user ID
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
                winner_position: winnerPosition, // Store position for token program compatibility
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

// 5. Create New Wager
app.post('/create-wager', async (req, res) => {
    try {
        const { wager_type, wager_data } = req.body;

        if (!wager_type || !wager_data) {
            return res.status(400).json({ error: 'wager_type and wager_data are required' });
        }

        console.log(`🔄 Creating new ${wager_type} wager`);

        // Validate wager data based on type
        if (wager_type === 'crypto') {
            const { creator_id, amount, token_symbol, prediction_type, target_price, expiry_time } = wager_data;

            if (!creator_id || !amount || !token_symbol || !prediction_type || !target_price || !expiry_time) {
                return res.status(400).json({ error: 'Missing required crypto wager fields' });
            }

            // Insert into crypto_wagers table
            const { data: wager, error: insertError } = await supabase
                .from('crypto_wagers')
                .insert({
                    creator_id,
                    amount,
                    token_symbol,
                    prediction_type,
                    target_price,
                    expiry_time,
                    status: 'open'
                })
                .select()
                .single();

            if (insertError) {
                console.error('❌ Error creating crypto wager:', insertError);
                return res.status(500).json({ error: 'Failed to create wager in database' });
            }

            // Create notification for creator
            await createNotification(creator_id, 'wager_created',
                'Wager Created!',
                `Your crypto wager on ${token_symbol} has been created. Waiting for someone to accept!`);

            console.log(`✅ Created crypto wager: ${wager.id}`);

            res.json({
                success: true,
                wager_id: wager.id,
                wager_type: 'crypto',
                status: 'open'
            });

        } else if (wager_type === 'sports') {
            const { creator_id, amount, sport, league, team1, team2, prediction, game_time, expiry_time } = wager_data;

            if (!creator_id || !amount || !sport || !league || !team1 || !team2 || !prediction || !game_time || !expiry_time) {
                return res.status(400).json({ error: 'Missing required sports wager fields' });
            }

            // Insert into sports_wagers table
            const { data: wager, error: insertError } = await supabase
                .from('sports_wagers')
                .insert({
                    creator_id,
                    amount,
                    sport,
                    league,
                    team1,
                    team2,
                    prediction,
                    game_time,
                    expiry_time,
                    status: 'open'
                })
                .select()
                .single();

            if (insertError) {
                console.error('❌ Error creating sports wager:', insertError);
                return res.status(500).json({ error: 'Failed to create wager in database' });
            }

            // Create notification for creator
            await createNotification(creator_id, 'wager_created',
                'Wager Created!',
                `Your sports wager on ${team1} vs ${team2} has been created. Waiting for someone to accept!`);

            console.log(`✅ Created sports wager: ${wager.id}`);

            res.json({
                success: true,
                wager_id: wager.id,
                wager_type: 'sports',
                status: 'open'
            });

        } else {
            return res.status(400).json({ error: 'Invalid wager_type. Must be "crypto" or "sports"' });
        }

    } catch (error) {
        console.error('❌ Error creating wager:', error);
        res.status(500).json({ error: error.message });
    }
});

// 6. Accept Wager
app.post('/accept-wager', async (req, res) => {
    try {
        const { wager_id, wager_type, acceptor_id } = req.body;

        if (!wager_id || !wager_type || !acceptor_id) {
            return res.status(400).json({ error: 'wager_id, wager_type, and acceptor_id are required' });
        }

        console.log(`🔄 Accepting ${wager_type} wager: ${wager_id}`);

        // Get wager from database
        const tableName = wager_type === 'crypto' ? 'crypto_wagers' : 'sports_wagers';
        const { data: wager, error: fetchError } = await supabase
            .from(tableName)
            .select('*')
            .eq('id', wager_id)
            .eq('status', 'open')
            .single();

        if (fetchError || !wager) {
            return res.status(404).json({ error: 'Wager not found or not open' });
        }

        if (wager.creator_id === acceptor_id) {
            return res.status(400).json({ error: 'Creator cannot accept their own wager' });
        }

        // Execute on-chain wager acceptance
        const acceptanceResult = await acceptWagerOnChain(
            wager_id,
            wager.creator_id,
            acceptor_id,
            wager.amount
        );

        if (!acceptanceResult.success) {
            return res.status(500).json({ error: acceptanceResult.error });
        }

        // Update database
        const { error: updateError } = await supabase
            .from(tableName)
            .update({
                status: 'active',
                acceptor_id: acceptor_id,
                on_chain_signature: acceptanceResult.signature
            })
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating wager ${wager_id}:`, updateError);
            return res.status(500).json({ error: 'Failed to update database' });
        }

        // Create notifications
        await createNotification(wager.creator_id, 'wager_accepted',
            'Wager Accepted!',
            `Your ${wager_type} wager has been accepted! The wager is now active.`);

        await createNotification(acceptor_id, 'wager_accepted',
            'Wager Accepted!',
            `You've accepted a ${wager_type} wager! The wager is now active.`);

        console.log(`✅ Accepted ${wager_type} wager ${wager_id}`);

        res.json({
            success: true,
            wager_id,
            wager_type,
            status: 'active',
            on_chain_signature: acceptanceResult.signature
        });

    } catch (error) {
        console.error('❌ Error accepting wager:', error);
        res.status(500).json({ error: error.message });
    }
});

// ON-CHAIN INTEGRATION FUNCTIONS

// Resolve crypto wager on-chain
async function resolveCryptoWagerOnChain(wagerId, winnerPosition, creatorId, acceptorId, amount) {
    try {
        console.log(`🔗 Executing on-chain resolution for wager ${wagerId}`);

        // Calculate fees and amounts
        const totalWagerAmount = amount * 2; // Both users put up 'amount' SOL
        const platformFee = totalWagerAmount * PLATFORM_FEE_PERCENTAGE; // 4% platform fee
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee
        const winnerAmount = totalWagerAmount - platformFee - networkFee; // Winner gets remaining amount

        console.log(`💰 Fee breakdown for wager ${wagerId}:`);
        console.log(`   Total wager: ${totalWagerAmount} SOL`);
        console.log(`   Platform fee (4%): ${platformFee} SOL`);
        console.log(`   Network fee: ${networkFee} SOL`);
        console.log(`   Winner gets: ${winnerAmount} SOL`);
        console.log(`   Treasury gets: ${platformFee} SOL`);

        // TODO: Implement actual Solana transaction
        // This would:
        // 1. Transfer platform fee to treasury wallet
        // 2. Pay network fee from escrow
        // 3. Transfer remaining amount to winner
        // 4. Close escrow account
        // const winnerType = winnerPosition === 'creator' ? { creator: {} } : { acceptor: {} };
        // const transaction = await program.methods.resolveWager(winnerType).accounts({
        //     wager: wagerAccount,
        //     escrow: escrowAccount,
        //     winner: winnerWallet,
        //     treasury: TREASURY_WALLET,
        //     authority: authorityKeypair.publicKey,
        // }).rpc();

        return {
            success: true,
            signature: 'mock_signature_' + Date.now(),
            feeBreakdown: {
                totalWager: totalWagerAmount,
                platformFee: platformFee,
                networkFee: networkFee,
                winnerAmount: winnerAmount,
                treasuryAmount: platformFee
            }
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
async function resolveSportsWagerOnChain(wagerId, winnerPosition, creatorId, acceptorId, amount) {
    try {
        console.log(`🔗 Executing on-chain sports resolution for wager ${wagerId}`);

        // Calculate fees and amounts (same as crypto)
        const totalWagerAmount = amount * 2; // Both users put up 'amount' SOL
        const platformFee = totalWagerAmount * PLATFORM_FEE_PERCENTAGE; // 4% platform fee
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee
        const winnerAmount = totalWagerAmount - platformFee - networkFee; // Winner gets remaining amount

        console.log(`💰 Fee breakdown for sports wager ${wagerId}:`);
        console.log(`   Total wager: ${totalWagerAmount} SOL`);
        console.log(`   Platform fee (4%): ${platformFee} SOL`);
        console.log(`   Network fee: ${networkFee} SOL`);
        console.log(`   Winner gets: ${winnerAmount} SOL`);
        console.log(`   Treasury gets: ${platformFee} SOL`);

        // TODO: Implement actual Solana transaction
        // This would:
        // 1. Transfer platform fee to treasury wallet
        // 2. Pay network fee from escrow
        // 3. Transfer remaining amount to winner
        // 4. Close escrow account
        // const winnerType = winnerPosition === 'creator' ? { creator: {} } : { acceptor: {} };
        // const transaction = await program.methods.resolveWager(winnerType).accounts({
        //     wager: wagerAccount,
        //     escrow: escrowAccount,
        //     winner: winnerWallet,
        //     treasury: TREASURY_WALLET,
        //     authority: authorityKeypair.publicKey,
        // }).rpc();

        return {
            success: true,
            signature: 'mock_signature_' + Date.now(),
            feeBreakdown: {
                totalWager: totalWagerAmount,
                platformFee: platformFee,
                networkFee: networkFee,
                winnerAmount: winnerAmount,
                treasuryAmount: platformFee
            }
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

        // For draws: NO platform fee, but network fee is split between users
        const totalWagerAmount = amount * 2; // Both users put up 'amount' SOL
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee
        const refundPerUser = amount - (networkFee / 2); // Each user gets their amount minus half the network fee

        console.log(`💰 Draw refund breakdown for wager ${wagerId}:`);
        console.log(`   Total wager: ${totalWagerAmount} SOL`);
        console.log(`   Platform fee: 0 SOL (no fee on draws)`);
        console.log(`   Network fee: ${networkFee} SOL (split between users)`);
        console.log(`   Creator refund: ${refundPerUser} SOL`);
        console.log(`   Acceptor refund: ${refundPerUser} SOL`);
        console.log(`   Treasury gets: 0 SOL (no platform fee on draws)`);

        // TODO: Implement actual Solana transaction
        // This would:
        // 1. Pay network fee from escrow
        // 2. Refund creator their amount (minus half network fee)
        // 3. Refund acceptor their amount (minus half network fee)
        // 4. Close escrow account
        // const transaction = await program.methods.handleDraw(creatorRefund, acceptorRefund).accounts({
        //     escrow: escrowAccount,
        //     creator: creatorWallet,
        //     acceptor: acceptorWallet,
        //     authority: authorityKeypair.publicKey,
        //     systemProgram: SystemProgram.programId
        // }).rpc();

        return {
            success: true,
            signature: 'mock_draw_signature_' + Date.now(),
            feeBreakdown: {
                totalWager: totalWagerAmount,
                platformFee: 0,
                networkFee: networkFee,
                creatorRefund: refundPerUser,
                acceptorRefund: refundPerUser
            }
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

        // For cancellations: NO platform fee, but network fee is paid from creator's refund
        const totalWagerAmount = amount * 2; // Both users put up 'amount' SOL
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee
        const creatorRefund = amount - networkFee; // Creator gets their amount minus network fee

        console.log(`💰 Cancellation refund breakdown for wager ${wagerId}:`);
        console.log(`   Total wager: ${totalWagerAmount} SOL`);
        console.log(`   Platform fee: 0 SOL (no fee on cancellations)`);
        console.log(`   Network fee: ${networkFee} SOL (paid from creator's refund)`);
        console.log(`   Creator refund: ${creatorRefund} SOL`);
        console.log(`   Acceptor refund: ${amount} SOL (full amount)`);
        console.log(`   Treasury gets: 0 SOL (no platform fee on cancellations)`);

        // TODO: Implement actual Solana transaction
        // This would:
        // 1. Pay network fee from escrow
        // 2. Refund creator their amount (minus network fee)
        // 3. Refund acceptor their full amount
        // 4. Close escrow account
        // const transaction = await program.methods.cancelWager(creatorRefund).accounts({
        //     escrow: escrowAccount,
        //     creator: creatorWallet,
        //     acceptor: acceptorWallet,
        //     authority: authorityKeypair.publicKey,
        //     systemProgram: SystemProgram.programId
        // }).rpc();

        return {
            success: true,
            signature: 'mock_cancel_signature_' + Date.now(),
            feeBreakdown: {
                totalWager: totalWagerAmount,
                platformFee: 0,
                networkFee: networkFee,
                creatorRefund: creatorRefund,
                acceptorRefund: amount
            }
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

        // For expirations: NO platform fee, but network fee is paid from creator's refund
        const totalWagerAmount = amount * 2; // Both users put up 'amount' SOL
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee
        const creatorRefund = amount - networkFee; // Creator gets their amount minus network fee

        console.log(`💰 Expiration refund breakdown for wager ${wagerId}:`);
        console.log(`   Total wager: ${totalWagerAmount} SOL`);
        console.log(`   Platform fee: 0 SOL (no fee on expirations)`);
        console.log(`   Network fee: ${networkFee} SOL (paid from creator's refund)`);
        console.log(`   Creator refund: ${creatorRefund} SOL`);
        console.log(`   Acceptor refund: ${amount} SOL (full amount)`);
        console.log(`   Treasury gets: 0 SOL (no platform fee on expirations)`);

        // TODO: Implement actual Solana transaction
        // This would:
        // 1. Pay network fee from escrow
        // 2. Refund creator their amount (minus network fee)
        // 3. Refund acceptor their full amount
        // 4. Close escrow account
        // const transaction = await program.methods.handleExpiredWager(creatorRefund).accounts({
        //     escrow: escrowAccount,
        //     creator: creatorWallet,
        //     acceptor: acceptorWallet,
        //     authority: authorityKeypair.publicKey,
        //     systemProgram: SystemProgram.programId
        // }).rpc();

        return {
            success: true,
            signature: 'mock_expire_signature_' + Date.now(),
            feeBreakdown: {
                totalWager: totalWagerAmount,
                platformFee: 0,
                networkFee: networkFee,
                creatorRefund: creatorRefund,
                acceptorRefund: amount
            }
        };
    } catch (error) {
        console.error('❌ On-chain expiration handling failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Accept wager on-chain
async function acceptWagerOnChain(wagerId, creatorId, acceptorId, amount) {
    try {
        console.log(`🔗 Executing on-chain wager acceptance for wager ${wagerId}`);

        // For acceptance: NO fees, just transfer to escrow
        const totalWagerAmount = amount * 2; // Both users put up 'amount' SOL
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee

        console.log(`💰 Wager acceptance breakdown for wager ${wagerId}:`);
        console.log(`   Total wager: ${totalWagerAmount} SOL`);
        console.log(`   Platform fee: 0 SOL (no fee on acceptance)`);
        console.log(`   Network fee: ${networkFee} SOL (paid by acceptor)`);
        console.log(`   Escrow holds: ${totalWagerAmount} SOL`);
        console.log(`   Treasury gets: 0 SOL (no platform fee on acceptance)`);

        // TODO: Implement actual Solana transaction
        // This would:
        // 1. Create escrow account (if not exists)
        // 2. Transfer acceptor's SOL to escrow
        // 3. Creator's SOL is already in escrow from creation
        // 4. Pay network fee from acceptor's wallet (not from escrow)
        // const transaction = await program.methods.acceptWager().accounts({
        //     escrow: escrowAccount,
        //     acceptor: acceptorWallet,
        //     authority: authorityKeypair.publicKey,
        //     systemProgram: SystemProgram.programId
        // }).rpc();

        return {
            success: true,
            signature: 'mock_accept_signature_' + Date.now(),
            feeBreakdown: {
                totalWager: totalWagerAmount,
                platformFee: 0,
                networkFee: networkFee,
                escrowAmount: totalWagerAmount
            }
        };
    } catch (error) {
        console.error('❌ On-chain wager acceptance failed:', error);
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
        if (!process.env.COINMARKETCAP_API_KEY) {
            throw new Error('COINMARKETCAP_API_KEY not found in environment variables');
        }

        console.log(`💰 Fetching real-time price for ${symbol} from CoinMarketCap`);

        const response = await fetch(`https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=${symbol}`, {
            headers: { 'X-CMC_PRO_API_KEY': process.env.COINMARKETCAP_API_KEY }
        });

        if (!response.ok) {
            throw new Error(`CoinMarketCap API responded with status: ${response.status}`);
        }

        const data = await response.json();

        if (data.data && data.data[symbol] && data.data[symbol].quote && data.data[symbol].quote.USD) {
            const price = data.data[symbol].quote.USD.price;
            console.log(`✅ ${symbol} price: $${price}`);
            return price;
        } else {
            throw new Error(`No valid price data found for ${symbol}`);
        }

    } catch (error) {
        console.error('❌ Error fetching crypto price:', error);
        throw new Error(`Failed to fetch price for ${symbol}: ${error.message}`);
    }
}

// Get sports game result from Sports API
async function getSportsGameResult(sport, team1, team2) {
    try {
        if (!process.env.SPORTS_API_KEY) {
            throw new Error('SPORTS_API_KEY not found in environment variables');
        }

        console.log(`🏈 Fetching real-time result for ${team1} vs ${team2} (${sport})`);

        // Using API-Football implementation (you can adapt for your specific sports API provider)
        const response = await fetch(`https://v3.football.api-sports.io/fixtures`, {
            method: 'GET',
            headers: {
                'x-rapidapi-host': 'v3.football.api-sports.io',
                'x-rapidapi-key': process.env.SPORTS_API_KEY
            }
        });

        if (!response.ok) {
            throw new Error(`Sports API responded with status: ${response.status}`);
        }

        const data = await response.json();

        // Search for the specific fixture
        if (data.response && data.response.length > 0) {
            // Find the most recent fixture between these teams
            const fixture = data.response.find(f =>
                (f.teams.home.name === team1 && f.teams.away.name === team2) ||
                (f.teams.home.name === team2 && f.teams.away.name === team1)
            );

            if (fixture && fixture.goals && fixture.goals.home !== null && fixture.goals.away !== null) {
                let result;
                if (fixture.goals.home > fixture.goals.away) {
                    result = fixture.teams.home.name === team1 ? team1 : team2;
                } else if (fixture.goals.away > fixture.goals.home) {
                    result = fixture.teams.away.name === team1 ? team1 : team2;
                } else {
                    result = 'draw';
                }

                console.log(`✅ ${team1} vs ${team2}: ${result} (${fixture.goals.home}-${fixture.goals.away})`);
                return result;
            } else {
                throw new Error(`No completed fixture found for ${team1} vs ${team2}`);
            }
        } else {
            throw new Error(`No fixtures found for ${team1} vs ${team2}`);
        }

    } catch (error) {
        console.error('❌ Error fetching sports result:', error);
        throw new Error(`Failed to fetch result for ${team1} vs ${team2}: ${error.message}`);
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

// CRYPTO TOKEN ENDPOINTS
// Get token list from CoinMarketCap
app.post('/token-list', async (req, res) => {
    try {
        const { apiKey, limit = 100, listingStatus = 'active' } = req.body;

        if (!apiKey) {
            return res.status(400).json({ error: 'API key is required' });
        }

        console.log(`🪙 Fetching token list (limit: ${limit}, status: ${listingStatus})`);

        // Fetch from CoinMarketCap API
        const response = await fetch(
            `https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=${limit}&listing_status=${listingStatus}`,
            {
                headers: {
                    'X-CMC_PRO_API_KEY': apiKey,
                    'Accept': 'application/json'
                }
            }
        );

        if (!response.ok) {
            throw new Error(`CoinMarketCap API error: ${response.status}`);
        }

        const data = await response.json();
        console.log(`✅ Fetched ${data.data?.length || 0} tokens from CoinMarketCap`);

        res.json(data);

    } catch (error) {
        console.error('❌ Error in token-list endpoint:', error);
        res.status(500).json({ 
            error: 'Failed to fetch token list',
            details: error.message 
        });
    }
});

// Get token info by ID from CoinMarketCap
app.post('/token-info', async (req, res) => {
    try {
        const { apiKey, id } = req.body;

        if (!apiKey || !id) {
            return res.status(400).json({ error: 'API key and token ID are required' });
        }

        console.log(`🪙 Fetching token info for ID: ${id}`);

        // Fetch from CoinMarketCap API
        const response = await fetch(
            `https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?id=${id}`,
            {
                headers: {
                    'X-CMC_PRO_API_KEY': apiKey,
                    'Accept': 'application/json'
                }
            }
        );

        if (!response.ok) {
            throw new Error(`CoinMarketCap API error: ${response.status}`);
        }

        const data = await response.json();
        console.log(`✅ Fetched token info for ID: ${id}`);

        res.json(data);

    } catch (error) {
        console.error('❌ Error in token-info endpoint:', error);
        res.status(500).json({ 
            error: 'Failed to fetch token info',
            details: error.message 
        });
    }
});

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
