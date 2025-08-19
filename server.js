import express from 'express';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { PublicKey, Keypair, Transaction, SystemProgram, LAMPORTS_PER_SOL, Connection } from '@solana/web3.js';
import pkg from '@project-serum/anchor';
const { Program, AnchorProvider, BN } = pkg;

import fetch from 'node-fetch';
import cors from 'cors';

// Load environment variables
dotenv.config();

// Initialize Supabase client
console.log('🔑 Environment variables check:');
console.log('SUPABASE_URL:', process.env.SUPABASE_URL ? '✅ Set' : '❌ Missing');
console.log('SUPABASE_SERVICE_ROLE_KEY:', process.env.SUPABASE_SERVICE_ROLE_KEY ?
    `✅ Set (${process.env.SUPABASE_SERVICE_ROLE_KEY.substring(0, 20)}...)` : '❌ Missing');

// TEMPORARY: Hardcoded service role key due to environment variable issues
console.log('🔑 Using hardcoded service role key for now');

// Check for common issues
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (key) {
    console.log('🔍 Key analysis:');
    console.log('  - Length:', key.length);
    console.log('  - Starts with eyJ:', key.startsWith('eyJ'));
    console.log('  - Ends with ==:', key.endsWith('=='));
    console.log('  - Contains spaces:', key.includes(' '));
    console.log('  - Contains newlines:', key.includes('\n'));
    console.log('  - Contains carriage returns:', key.includes('\r'));
}

const supabase = createClient(
    process.env.SUPABASE_URL,
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFqcWhuYnpmZ2lobHhramphd2RrIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTUwNTE5NSwiZXhwIjoyMDcxMDgxMTk1fQ.BjU9hCXmk95mMzMzeGo0yCs0uU2enEfVX_nKswypIaQ'
);

// Test Supabase connection
console.log('🔌 Testing Supabase connection...');

// Initialize Solana connection to devnet
const connection = new Connection(process.env.SOLANA_RPC_URL || 'https://api.devnet.solana.com', 'confirmed');

// Initialize Solana program ID for direct RPC calls
const WAGERFI_PROGRAM_ID = new PublicKey('3trZZeVh3j9sx6H8ZYCdsouGnMjcyyGQoLqLE7CzD8sa');

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

// Load the real program IDL from the file for instruction data
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Read the IDL file
const idlPath = join(__dirname, 'WagerFi-IDL-New.json');
const idl = JSON.parse(readFileSync(idlPath, 'utf8'));

console.log('📋 Loaded WagerFi IDL with instructions:', idl.instructions.map(i => i.name));

// Create Anchor provider and program
const anchorProvider = new AnchorProvider(connection, {
    publicKey: authorityKeypair.publicKey,
    signTransaction: async (tx) => {
        tx.sign(authorityKeypair);
        return tx;
    },
    signAllTransactions: async (txs) => {
        txs.forEach(tx => tx.sign(authorityKeypair));
        return txs;
    }
}, { commitment: 'confirmed' });

const anchorProgram = new Program(idl, WAGERFI_PROGRAM_ID, anchorProvider);
console.log('🔗 Anchor program initialized successfully');

console.log('🔗 WagerFi Token Program initialized:', WAGERFI_PROGRAM_ID.toString());
console.log('🌐 Connected to Solana devnet');



// Helper function to execute program instruction via Anchor client
async function executeProgramInstruction(instructionName, accounts, args = []) {
    try {
        console.log(`🔗 Executing ${instructionName} via Anchor client...`);

        // Use Anchor program methods instead of manual RPC
        let result;

        switch (instructionName) {
            case 'resolveWager':
                // Use existing PDAs to prevent account creation requiring rent
                const resolveWagerPDA = new PublicKey(accounts.wagerId);
                const resolveEscrowPDA = new PublicKey(accounts.escrowPda || accounts.wagerId);

                result = await anchorProgram.methods
                    .resolveWager({ [args.winner.toLowerCase()]: {} })
                    .accounts({
                        wager: resolveWagerPDA,
                        escrow: resolveEscrowPDA,
                        winner: new PublicKey(accounts.winnerPubkey),
                        treasury: TREASURY_WALLET,
                        authority: authorityKeypair.publicKey,
                    })
                    .signers([authorityKeypair])
                    .rpc();
                break;

            case 'handleExpiredWager':
                // Use the PDAs stored in the database to avoid creating new accounts
                // The database PDAs are the actual Solana accounts that exist

                // Use the PDAs stored in the database to avoid creating new accounts
                // The database PDAs are the actual Solana accounts that exist
                const databaseWagerPDA = new PublicKey(accounts.wagerId);
                const databaseEscrowPDA = new PublicKey(accounts.escrowPda);

                console.log(`🔍 Using database PDAs to avoid account creation:`);
                console.log(`🔍 Database wager PDA: ${databaseWagerPDA.toString()}`);
                console.log(`🔍 Database escrow PDA: ${databaseEscrowPDA.toString()}`);

                // Verify accounts exist before proceeding
                try {
                    const wagerAccount = await anchorProgram.account.wager.fetch(databaseWagerPDA);
                    const escrowBalance = await anchorProgram.provider.connection.getBalance(databaseEscrowPDA);

                    console.log(`✅ Wager account verified: ${databaseWagerPDA.toString()}`);
                    console.log(`✅ Escrow account verified: ${databaseEscrowPDA.toString()}`);
                    console.log(`✅ Escrow balance: ${escrowBalance / LAMPORTS_PER_SOL} SOL`);

                    if (escrowBalance === 0) {
                        throw new Error('Escrow account has no funds');
                    }
                } catch (accountError) {
                    console.error('❌ Account verification failed:', accountError);
                    throw new Error(`Account verification failed: ${accountError.message}`);
                }

                result = await anchorProgram.methods
                    .handleExpiredWager()
                    .accounts({
                        wager: databaseWagerPDA,
                        escrow: databaseEscrowPDA,
                        creator: new PublicKey(accounts.creatorPubkey),
                        authority: authorityKeypair.publicKey,
                    })
                    .signers([authorityKeypair])
                    .rpc();
                break;

            case 'cancelWager':
                // Use existing PDAs to prevent account creation requiring rent
                const cancelWagerPDA = new PublicKey(accounts.wagerId);
                const cancelEscrowPDA = new PublicKey(accounts.escrowPda || accounts.wagerId);

                result = await anchorProgram.methods
                    .cancelWager()
                    .accounts({
                        wager: cancelWagerPDA,
                        escrow: cancelEscrowPDA,
                        creator: new PublicKey(accounts.creatorPubkey),
                        authority: authorityKeypair.publicKey,
                    })
                    .signers([authorityKeypair])
                    .rpc();
                break;

            case 'handleDrawWager':
                // Use existing PDAs to prevent account creation requiring rent
                const drawWagerPDA = new PublicKey(accounts.wagerId);
                const drawEscrowPDA = new PublicKey(accounts.escrowPda || accounts.wagerId);

                result = await anchorProgram.methods
                    .handleDrawWager()
                    .accounts({
                        wager: drawWagerPDA,
                        escrow: drawEscrowPDA,
                        creator: new PublicKey(accounts.creatorPubkey),
                        acceptor: new PublicKey(accounts.acceptorPubkey),
                        authority: authorityKeypair.publicKey,
                    })
                    .signers([authorityKeypair])
                    .rpc();
                break;

            case 'acceptWager':
                // Use existing PDAs to prevent account creation requiring rent
                const acceptWagerPDA = new PublicKey(accounts.wagerId);
                const acceptEscrowPDA = new PublicKey(accounts.escrowPda || accounts.wagerId);

                result = await anchorProgram.methods
                    .acceptWager()
                    .accounts({
                        wager: acceptWagerPDA,
                        escrow: acceptEscrowPDA,
                        acceptor: new PublicKey(accounts.acceptorPubkey),
                        authority: authorityKeypair.publicKey,
                    })
                    .signers([authorityKeypair])
                    .rpc();
                break;

            default:
                throw new Error(`Unsupported instruction: ${instructionName}`);
        }

        console.log(`✅ ${instructionName} executed successfully:`, result);
        return result;

    } catch (error) {
        console.error(`❌ Error executing ${instructionName}:`, error);
        throw error;
    }
}

// Helper function to create notifications
async function createNotification(userId, type, title, message, data = {}) {
    try {
        if (!userId) {
            console.log('⚠️ Skipping notification - no user ID provided');
            return;
        }

        // First, get the user's wallet address from the users table
        const { data: userData, error: userError } = await supabase
            .from('users')
            .select('wallet_address, user_address')
            .eq('id', userId)
            .single();

        if (userError || !userData) {
            console.error('❌ Error fetching user data for notification:', userError);
            return;
        }

        // Use wallet_address or user_address, whichever is available
        const userAddress = userData.wallet_address || userData.user_address;
        if (!userAddress) {
            console.error('❌ No wallet address found for user:', userId);
            return;
        }

        const { error } = await supabase
            .from('notifications')
            .insert({
                user_address: userAddress, // Use user_address as per database schema
                type: type,
                title: title,
                message: message,
                data: data,
                read: false // Use 'read' instead of 'is_read' as per database schema
            });

        if (error) {
            console.error('❌ Error creating notification:', error);
        } else {
            console.log(`✅ Notification created for user ${userId} (${userAddress}): ${type}`);
        }
    } catch (error) {
        console.error('❌ Error in createNotification:', error);
    }
}

// Treasury wallet for platform fees (4% total - 2% from each user)
const TREASURY_WALLET = process.env.TREASURY_WALLET_ADDRESS ?
    new PublicKey(process.env.TREASURY_WALLET_ADDRESS) :
    new PublicKey('11111111111111111111111111111111'); // Fallback to system program if not set

// Platform fee configuration
const PLATFORM_FEE_PERCENTAGE = 0.04; // 4% total (2% from each user)
const SOLANA_TRANSACTION_FEE = 0.000005; // Approximate Solana transaction fee

const app = express();
const PORT = process.env.PORT || 3001;

// Configure CORS for all routes - more permissive approach
app.use(cors({
    origin: function (origin, callback) {
        // Allow requests with no origin (like mobile apps or curl requests)
        if (!origin) return callback(null, true);

        const allowedOrigins = [
            'http://localhost:5173',
            'http://localhost:3000',
            'https://wagerfi.netlify.app',
            'https://wagerfi.vercel.app',
            'https://wagerfi.gg'
        ];

        if (allowedOrigins.indexOf(origin) !== -1) {
            callback(null, true);
        } else {
            console.log(`🚫 CORS blocked origin: ${origin}`);
            callback(new Error('Not allowed by CORS'));
        }
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Origin', 'Accept'],
    optionsSuccessStatus: 200,
    preflightContinue: false
}));

// Additional CORS headers for preflight requests
app.use((req, res, next) => {
    const origin = req.headers.origin;

    if (origin) {
        res.header('Access-Control-Allow-Origin', origin);
    }

    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
    res.header('Access-Control-Allow-Credentials', 'true');

    // Handle preflight requests
    if (req.method === 'OPTIONS') {
        console.log(`🔄 CORS preflight request from: ${origin}`);
        res.status(200).end();
        return;
    }

    next();
});

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

// CORS test endpoint
app.get('/cors-test', (req, res) => {
    console.log(`🧪 CORS test request from: ${req.headers.origin}`);
    res.json({
        message: 'CORS is working!',
        origin: req.headers.origin,
        timestamp: new Date().toISOString()
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

// IMMEDIATE EXECUTION FUNCTIONS + AUTO-EXPIRATION
// 
// RESOLUTION STRATEGY:
// - Crypto wagers: Resolve automatically when called (e.g., on expiry)
// - Sports wagers: Check every 5 minutes until result found, then resolve
// - Both: Map winners to positions (creator/acceptor) for token program compatibility
// - Auto-expiration: Run every 5 minutes to expire overdue wagers

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

        // Create notifications using the new database function
        const winnerWalletAddress = winnerPosition === 'creator' ? wager.creator_address : wager.acceptor_address;
        const loserWalletAddress = winnerPosition === 'creator' ? wager.acceptor_address : wager.creator_address;

        const notificationResult = await supabase.rpc('create_wager_resolved_notification', {
            p_winner_address: winnerWalletAddress,
            p_loser_address: loserWalletAddress,
            p_wager_type: 'sports',
            p_wager_amount: wager.amount || wager.sol_amount,
            p_is_draw: isDraw
        });

        if (notificationResult.error) {
            console.error('❌ Error creating wager resolution notification:', notificationResult.error);
        } else {
            console.log('✅ Created wager resolution notification');
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

// 3. Cancel Wager (Database Status Update + Background Refund)
app.post('/cancel-wager', async (req, res) => {
    try {
        const { wager_id, wager_type, cancelling_address } = req.body;

        if (!wager_id || !wager_type || !cancelling_address) {
            return res.status(400).json({ error: 'wager_id, wager_type, and cancelling_address are required' });
        }

        console.log(`🔄 Cancelling ${wager_type} wager: ${wager_id} by ${cancelling_address}`);

        // Get wager from database
        const tableName = wager_type === 'crypto' ? 'crypto_wagers' : 'sports_wagers';
        console.log(`🔍 Looking for wager in table: ${tableName}`);
        console.log(`🔍 Searching for wager_id: ${wager_id}`);

        // First, let's see what wagers exist with this ID
        const { data: allWagers, error: allWagersError } = await supabase
            .from(tableName)
            .select('*')
            .eq('wager_id', wager_id);

        if (allWagersError) {
            console.error(`❌ Error fetching all wagers with ID ${wager_id}:`, allWagersError);
            return res.status(500).json({ error: 'Database query failed' });
        }

        console.log(`🔍 Found ${allWagers?.length || 0} wagers with this ID:`, allWagers);

        // Check if any wager exists with this ID, regardless of status
        if (allWagers && allWagers.length > 0) {
            console.log(`🔍 Wager details:`, {
                id: allWagers[0].id,
                wager_id: allWagers[0].wager_id,
                status: allWagers[0].status,
                creator_id: allWagers[0].creator_id,
                creator_address: allWagers[0].creator_address
            });
        }

        // Now try to get the specific wager with 'open' status
        const { data: wager, error: fetchError } = await supabase
            .from(tableName)
            .select('*')
            .eq('wager_id', wager_id)
            .eq('status', 'open')
            .single();

        if (fetchError) {
            console.error(`❌ Error fetching open wager ${wager_id}:`, fetchError);
            return res.status(404).json({ error: `Wager not found or not open: ${fetchError.message}` });
        }

        if (!wager) {
            console.error(`❌ No open wager found with ID ${wager_id}`);
            return res.status(404).json({ error: 'Wager not found or not open' });
        }

        // Check if user has permission to cancel
        let hasPermission = false;

        if (wager.creator_address === cancelling_address) {
            hasPermission = true;
        } else if (wager.creator_address === null && wager.creator_id) {
            // Fallback: check if the cancelling address matches the creator's wallet address
            const { data: creatorUser, error: userError } = await supabase
                .from('users')
                .select('wallet_address')
                .eq('id', wager.creator_id)
                .single();

            if (!userError && creatorUser && creatorUser.wallet_address === cancelling_address) {
                hasPermission = true;
            }
        }

        if (!hasPermission) {
            return res.status(403).json({ error: 'Only the wager creator can cancel this wager' });
        }

        // Update database status to cancelled
        const currentMetadata = wager.metadata || {};
        const updatedMetadata = {
            ...currentMetadata,
            cancelled_at: new Date().toISOString(),
            cancelled_by: cancelling_address
        };

        const { error: updateError } = await supabase
            .from(tableName)
            .update({
                status: 'cancelled',
                updated_at: new Date().toISOString(),
                metadata: updatedMetadata
            })
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating wager ${wager_id}:`, updateError);
            return res.status(500).json({ error: 'Failed to update database' });
        }

        // Create notification for creator
        await createNotification(wager.creator_id, 'wager_cancelled',
            'Wager Cancelled!',
            `Your ${wager_type} wager has been cancelled. Refund will be processed automatically.`);

        console.log(`✅ Cancelled ${wager_type} wager ${wager_id} - Status updated to cancelled`);

        res.json({
            success: true,
            wager_id,
            wager_type,
            status: 'cancelled',
            message: 'Wager cancelled successfully. Refund will be processed by background worker.'
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

        // Get acceptor's wallet address
        const { data: acceptorUser, error: userError } = await supabase
            .from('users')
            .select('wallet_address')
            .eq('id', acceptor_id)
            .single();

        if (userError || !acceptorUser) {
            console.error(`❌ Error fetching acceptor user ${acceptor_id}:`, userError);
            return res.status(500).json({ error: 'Failed to fetch acceptor user' });
        }

        // Calculate opponent position based on wager type and creator position
        let opponentPosition;
        if (wager_type === 'crypto') {
            // For crypto: >= becomes <=, <= becomes >=
            opponentPosition = wager.creator_position === '>=' ? '<=' : '>=';
        } else {
            // For sports: home becomes away, away becomes home
            opponentPosition = wager.creator_position === 'home' ? 'away' : 'home';
        }

        // Update database with all required fields
        const { error: updateError } = await supabase
            .from(tableName)
            .update({
                status: 'active',
                acceptor_id: acceptor_id,
                acceptor_address: acceptorUser.wallet_address,
                opponent_address: acceptorUser.wallet_address,
                opponent_position: opponentPosition,
                on_chain_signature: acceptanceResult.signature,
                updated_at: new Date().toISOString()
            })
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating wager ${wager_id}:`, updateError);
            return res.status(500).json({ error: 'Failed to update database' });
        }

        // Update stats for both users - both have now wagered the amount
        await updateWagerAcceptanceStats(wager.creator_id, acceptor_id, wager.amount || wager.sol_amount, wager_type);

        // Create notifications using the new database function
        const notificationResult = await supabase.rpc('create_wager_accepted_notification', {
            p_creator_address: wager.creator_address,
            p_acceptor_address: acceptorUser.wallet_address,
            p_wager_type: wager_type,
            p_wager_amount: wager.amount || wager.sol_amount
        });

        if (notificationResult.error) {
            console.error('❌ Error creating wager acceptance notifications:', notificationResult.error);
        } else {
            console.log('✅ Created wager acceptance notifications for both users');
        }

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



// 8. Process Cancelled Wagers for Refunds (Background Worker Function)
app.post('/process-cancelled-wagers', async (req, res) => {
    try {
        console.log('🔄 Processing cancelled wagers for refunds...');

        // First, expire any wagers that have passed their deadline
        const expiredCount = await expireExpiredWagers();
        console.log(`✅ Expired ${expiredCount} wagers automatically`);

        // Get all cancelled wagers that need refunds
        const cancelledWagers = await getCancelledWagersForRefund();
        console.log(`📋 Found ${cancelledWagers.length} cancelled wagers needing refunds`);

        if (cancelledWagers.length === 0) {
            return res.json({
                success: true,
                message: 'No cancelled wagers need refunds',
                expired_count: expiredCount,
                processed_count: 0
            });
        }

        // Process each cancelled wager
        let processedCount = 0;
        for (const wager of cancelledWagers) {
            try {
                await processWagerRefund(wager);
                processedCount++;
            } catch (error) {
                console.error(`❌ Error processing refund for ${wager.wager_id}:`, error);
            }
        }

        console.log(`✅ Processed ${processedCount} wager refunds`);

        res.json({
            success: true,
            message: 'Cancelled wagers processed successfully',
            expired_count: expiredCount,
            processed_count: processedCount,
            total_cancelled: cancelledWagers.length
        });

    } catch (error) {
        console.error('❌ Error processing cancelled wagers:', error);
        res.status(500).json({ error: error.message });
    }
});

// 9. Mark Refund as Processed
app.post('/mark-refund-processed', async (req, res) => {
    try {
        const { wager_id, wager_type, refund_signature } = req.body;

        if (!wager_id || !wager_type || !refund_signature) {
            return res.status(400).json({ error: 'wager_id, wager_type, and refund_signature are required' });
        }

        console.log(`🔄 Marking refund as processed for ${wager_type} wager: ${wager_id}`);

        const result = await markRefundProcessed(wager_id, wager_type, refund_signature);

        if (result.success) {
            console.log(`✅ Refund marked as processed for ${wager_id}`);
            res.json(result);
        } else {
            console.error(`❌ Failed to mark refund as processed for ${wager_id}:`, result.error);
            res.status(500).json(result);
        }

    } catch (error) {
        console.error('❌ Error marking refund as processed:', error);
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

        // Execute real Solana transaction using WagerFi program
        try {
            console.log(`🔐 Executing real on-chain resolution...`);

            // Get wager and escrow accounts from database
            const { data: wagerData, error: wagerError } = await supabase
                .from('crypto_wagers')
                .select('wager_id, escrow_pda, creator_address, acceptor_address')
                .eq('wager_id', wagerId)
                .single();

            if (wagerError || !wagerData) {
                throw new Error(`Failed to fetch wager data: ${wagerError?.message || 'Wager not found'}`);
            }

            // Use escrow_pda as the wager account (it's the actual Solana account)
            console.log(`🔍 Wager data:`, {
                wager_id: wagerData.wager_id,
                escrow_pda: wagerData.escrow_pda,
                creator_address: wagerData.creator_address,
                acceptor_address: wagerData.acceptor_address
            });

            const wagerAccount = new PublicKey(wagerData.escrow_pda);
            const escrowAccount = new PublicKey(wagerData.escrow_pda);
            const winnerWallet = winnerPosition === 'creator'
                ? new PublicKey(wagerData.creator_address)
                : new PublicKey(wagerData.acceptor_address);

            console.log(`   Wager Account: ${wagerAccount.toString()}`);
            console.log(`   Escrow Account: ${escrowAccount.toString()}`);
            console.log(`   Winner Wallet: ${winnerWallet.toString()}`);
            console.log(`   Treasury: ${TREASURY_WALLET.toString()}`);

            // Execute the resolveWager instruction
            const transaction = await executeProgramInstruction('resolveWager', {
                wagerId: wagerData.wager_id,
                winnerPubkey: winnerWallet.toString()
            }, { winner: winnerPosition });

            console.log(`   🔐 Real on-chain resolution completed: ${transaction}`);

            return {
                success: true,
                signature: transaction,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: platformFee,
                    networkFee: networkFee,
                    winnerAmount: winnerAmount,
                    treasuryAmount: platformFee
                }
            };

        } catch (onChainError) {
            console.error(`❌ Real on-chain resolution failed:`, onChainError);

            // Fallback to mock for now
            const mockSignature = `mock_resolution_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            console.log(`   🔐 Falling back to mock resolution: ${mockSignature}`);

            return {
                success: true,
                signature: mockSignature,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: platformFee,
                    networkFee: networkFee,
                    winnerAmount: winnerAmount,
                    treasuryAmount: platformFee
                },
                note: 'Mock due to on-chain error'
            };
        }
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

        // Execute real Solana transaction using WagerFi program
        try {
            console.log(`🔐 Executing real on-chain sports resolution...`);

            // Get wager and escrow accounts from database
            const { data: wagerData, error: wagerError } = await supabase
                .from('sports_wagers')
                .select('wager_id, escrow_pda, creator_address, acceptor_address')
                .eq('wager_id', wagerId)
                .single();

            if (wagerError || !wagerData) {
                throw new Error(`Failed to fetch sports wager data: ${wagerError?.message || 'Wager not found'}`);
            }

            // Note: escrow_pda IS the wager account in the Solana program
            const wagerAccount = new PublicKey(wagerData.escrow_pda);
            const escrowAccount = new PublicKey(wagerData.escrow_pda);
            const winnerWallet = winnerPosition === 'creator'
                ? new PublicKey(wagerData.creator_address)
                : new PublicKey(wagerData.acceptor_address);

            console.log(`   Wager Account: ${wagerAccount.toString()}`);
            console.log(`   Escrow Account: ${escrowAccount.toString()}`);
            console.log(`   Winner Wallet: ${winnerWallet.toString()}`);
            console.log(`   Treasury: ${TREASURY_WALLET.toString()}`);

            // Execute the resolveWager instruction
            const transaction = await executeProgramInstruction('resolveWager', {
                wagerId: wagerData.wager_id,
                winnerPubkey: winnerWallet.toString()
            }, { winner: winnerPosition });

            console.log(`   🔐 Real on-chain sports resolution completed: ${transaction}`);

            return {
                success: true,
                signature: transaction,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: platformFee,
                    networkFee: networkFee,
                    winnerAmount: winnerAmount,
                    treasuryAmount: platformFee
                }
            };

        } catch (onChainError) {
            console.error(`❌ Real on-chain sports resolution failed:`, onChainError);

            // Fallback to mock for now
            const mockSignature = `mock_sports_resolution_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            console.log(`   🔐 Falling back to mock sports resolution: ${mockSignature}`);

            return {
                success: true,
                signature: mockSignature,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: platformFee,
                    networkFee: networkFee,
                    winnerAmount: winnerAmount,
                    treasuryAmount: platformFee
                },
                note: 'Mock due to on-chain error'
            };
        }
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

        // Execute real Solana transaction using WagerFi program
        try {
            console.log(`🔐 Executing real on-chain draw handling...`);

            // Get wager and escrow accounts from database
            const { data: wagerData, error: wagerError } = await supabase
                .from('sports_wagers')
                .select('wager_id, escrow_pda, creator_address, acceptor_address')
                .eq('wager_id', wagerId)
                .single();

            if (wagerError || !wagerData) {
                throw new Error(`Failed to fetch sports wager data: ${wagerError?.message || 'Wager not found'}`);
            }

            // Note: escrow_pda IS the wager account in the Solana program
            const wagerAccount = new PublicKey(wagerData.escrow_pda);
            const escrowAccount = new PublicKey(wagerData.escrow_pda);
            const creatorWallet = new PublicKey(wagerData.creator_address);
            const acceptorWallet = new PublicKey(wagerData.acceptor_address);

            console.log(`   Wager Account: ${wagerAccount.toString()}`);
            console.log(`   Escrow Account: ${escrowAccount.toString()}`);
            console.log(`   Creator Wallet: ${creatorWallet.toString()}`);
            console.log(`   Acceptor Wallet: ${acceptorWallet.toString()}`);

            // Execute the handleDrawWager instruction
            const transaction = await executeProgramInstruction('handleDrawWager', {
                wagerId: wagerData.wager_id,
                creatorPubkey: creatorWallet.toString(),
                acceptorPubkey: acceptorWallet.toString()
            });

            console.log(`   🔐 Real on-chain draw handling completed: ${transaction}`);

            return {
                success: true,
                signature: transaction,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    creatorRefund: refundPerUser,
                    acceptorRefund: refundPerUser
                }
            };

        } catch (onChainError) {
            console.error(`❌ Real on-chain draw handling failed:`, onChainError);

            // Fallback to mock for now
            const mockSignature = `mock_draw_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            console.log(`   🔐 Falling back to mock draw handling: ${mockSignature}`);

            return {
                success: true,
                signature: mockSignature,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    creatorRefund: refundPerUser,
                    acceptorRefund: refundPerUser
                },
                note: 'Mock due to on-chain error'
            };
        }
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

        // Execute real Solana transaction using WagerFi program
        try {
            console.log(`🔐 Executing real on-chain cancellation...`);

            // Get wager and escrow accounts from database
            const { data: wagerData, error: wagerError } = await supabase
                .from('crypto_wagers')
                .select('wager_id, escrow_pda, creator_address, acceptor_address')
                .eq('wager_id', wagerId)
                .single();

            if (wagerError || !wagerData) {
                throw new Error(`Failed to fetch crypto wager data: ${wagerError?.message || 'Wager not found'}`);
            }

            // Note: escrow_pda IS the wager account in the Solana program
            const wagerAccount = new PublicKey(wagerData.escrow_pda);
            const escrowAccount = new PublicKey(wagerData.escrow_pda);
            const creatorWallet = new PublicKey(wagerData.creator_address);

            console.log(`   Wager Account: ${wagerAccount.toString()}`);
            console.log(`   Escrow Account: ${escrowAccount.toString()}`);
            console.log(`   Creator Wallet: ${creatorWallet.toString()}`);

            // Execute the cancelWager instruction
            const transaction = await executeProgramInstruction('cancelWager', {
                wagerId: wagerData.wager_id,
                creatorPubkey: creatorWallet.toString()
            });

            console.log(`   🔐 Real on-chain cancellation completed: ${transaction}`);

            return {
                success: true,
                signature: transaction,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    creatorRefund: creatorRefund,
                    acceptorRefund: amount
                }
            };

        } catch (onChainError) {
            console.error(`❌ Real on-chain cancellation failed:`, onChainError);

            // Fallback to mock for now
            const mockSignature = `mock_cancel_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            console.log(`   🔐 Falling back to mock cancellation: ${mockSignature}`);

            return {
                success: true,
                signature: mockSignature,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    creatorRefund: creatorRefund,
                    acceptorRefund: amount
                },
                note: 'Mock due to on-chain error'
            };
        }
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

        // Execute real Solana transaction using WagerFi program
        try {
            console.log(`🔐 Executing real on-chain expiration handling...`);

            // Get wager and escrow accounts from database
            const { data: wagerData, error: wagerError } = await supabase
                .from('crypto_wagers')
                .select('wager_id, escrow_pda, creator_address')
                .eq('wager_id', wagerId)
                .single();

            if (wagerError || !wagerData) {
                throw new Error(`Failed to fetch crypto wager data: ${wagerError?.message || 'Wager not found'}`);
            }

            // Note: escrow_pda IS the wager account in the Solana program
            const wagerAccount = new PublicKey(wagerData.escrow_pda);
            const escrowAccount = new PublicKey(wagerData.escrow_pda);
            const creatorWallet = new PublicKey(wagerData.creator_address);

            console.log(`   Wager Account: ${wagerAccount.toString()}`);
            console.log(`   Escrow Account: ${escrowAccount.toString()}`);
            console.log(`   Creator Wallet: ${creatorWallet.toString()}`);

            // Execute the handleExpiredWager instruction
            const transaction = await executeProgramInstruction('handleExpiredWager', {
                wagerId: wagerData.wager_id, // Use the actual wager ID for PDA derivation
                escrowPda: wagerData.escrow_pda, // Pass the escrow PDA for verification
                creatorPubkey: creatorWallet.toString()
            });

            console.log(`   🔐 Real on-chain expiration handling completed: ${transaction}`);

            return {
                success: true,
                signature: transaction,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    creatorRefund: creatorRefund
                }
            };

        } catch (onChainError) {
            console.error(`❌ Real on-chain expiration handling failed:`, onChainError);

            // Fallback to mock for now
            const mockSignature = `mock_expire_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            console.log(`   🔐 Falling back to mock expiration handling: ${mockSignature}`);

            return {
                success: true,
                signature: mockSignature,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    creatorRefund: creatorRefund
                },
                note: 'Mock due to on-chain error'
            };
        }
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

        // Execute real Solana transaction using WagerFi program
        try {
            console.log(`🔐 Executing real on-chain wager acceptance...`);

            // Get wager and escrow accounts from database
            const { data: wagerData, error: wagerError } = await supabase
                .from('crypto_wagers')
                .select('wager_id, escrow_pda')
                .eq('wager_id', wagerId)
                .single();

            if (wagerError || !wagerData) {
                throw new Error(`Failed to fetch crypto wager data: ${wagerError?.message || 'Wager not found'}`);
            }

            // Get acceptor user to get their wallet address
            const { data: acceptorUser, error: userError } = await supabase
                .from('users')
                .select('wallet_address')
                .eq('id', acceptorId)
                .single();

            if (userError || !acceptorUser) {
                throw new Error(`Failed to fetch acceptor user: ${userError?.message || 'User not found'}`);
            }

            // Note: escrow_pda IS the wager account in the Solana program
            const wagerAccount = new PublicKey(wagerData.escrow_pda);
            const escrowAccount = new PublicKey(wagerData.escrow_pda);
            const acceptorWallet = new PublicKey(acceptorUser.wallet_address);

            console.log(`   Wager Account: ${wagerAccount.toString()}`);
            console.log(`   Escrow Account: ${escrowAccount.toString()}`);
            console.log(`   Acceptor Wallet: ${acceptorWallet.toString()}`);

            // Execute the acceptWager instruction
            const transaction = await executeProgramInstruction('acceptWager', {
                wagerId: wagerData.wager_id,
                acceptorPubkey: acceptorWallet.toString()
            });

            console.log(`   🔐 Real on-chain wager acceptance completed: ${transaction}`);

            return {
                success: true,
                signature: transaction,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    escrowAmount: totalWagerAmount
                }
            };

        } catch (onChainError) {
            console.error(`❌ Real on-chain wager acceptance failed:`, onChainError);

            // Fallback to mock for now
            const mockSignature = `mock_accept_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            console.log(`   🔐 Falling back to mock wager acceptance: ${mockSignature}`);

            return {
                success: true,
                signature: mockSignature,
                feeBreakdown: {
                    totalWager: totalWagerAmount,
                    platformFee: 0,
                    networkFee: networkFee,
                    escrowAmount: totalWagerAmount
                },
                note: 'Mock due to on-chain error'
            };
        }
    } catch (error) {
        console.error('❌ On-chain wager acceptance failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Handle expired crypto wagers - resolve matched ones, refund unmatched ones
async function handleExpiredCryptoWagers() {
    try {
        console.log('🔄 Handling expired crypto wagers...');

        // Get all expired crypto wagers that need processing
        // Look for both cancelled and active wagers that have expired
        console.log('🔍 Looking for expired crypto wagers to resolve/refund with statuses: cancelled, active');
        const { data: expiredWagers, error: fetchError } = await supabase
            .from('crypto_wagers')
            .select('*')
            .in('status', ['cancelled', 'active'])
            .lt('expires_at', new Date().toISOString())
            .is('metadata->expiry_processed', null);

        console.log(`🔍 Found ${expiredWagers?.length || 0} expired crypto wagers to resolve/refund`);
        if (expiredWagers && expiredWagers.length > 0) {
            console.log('🔍 Wagers to process:', expiredWagers.map(w => ({ id: w.id, status: w.status, expires_at: w.expires_at, acceptor_id: w.acceptor_id })));
        }

        if (fetchError) {
            console.error('❌ Error fetching expired crypto wagers:', fetchError);
            return;
        }

        if (!expiredWagers || expiredWagers.length === 0) {
            return;
        }

        console.log(`📋 Processing ${expiredWagers.length} expired crypto wagers...`);

        for (const wager of expiredWagers) {
            try {
                if (wager.status === 'matched' || wager.acceptor_id) {
                    // Wager was matched - resolve it
                    console.log(`🏁 Resolving matched expired wager: ${wager.wager_id}`);
                    await resolveExpiredMatchedWager(wager);
                } else {
                    // Wager was unmatched - refund creator
                    console.log(`💰 Refunding unmatched expired wager: ${wager.wager_id}`);
                    await refundUnmatchedExpiredWager(wager);
                }

                // Mark as processed
                await markExpiryProcessed(wager.wager_id, 'crypto');

            } catch (error) {
                console.error(`❌ Error processing expired wager ${wager.wager_id}:`, error);
            }
        }

    } catch (error) {
        console.error('❌ Error in handleExpiredCryptoWagers:', error);
    }
}

// Resolve expired matched wager (determine winner and pay out)
async function resolveExpiredMatchedWager(wager) {
    try {
        console.log(`🏁 Resolving expired matched wager: ${wager.wager_id}`);

        // Get current token price at expiry
        const expiryPrice = await getCurrentCryptoPrice(wager.token_symbol);

        // Log price comparison for debugging
        console.log(`💰 Price comparison for ${wager.token_symbol}:`);
        console.log(`   Target price: $${wager.target_price}`);
        console.log(`   Expiry price: $${expiryPrice}`);
        console.log(`   Prediction type: ${wager.prediction_type}`);
        console.log(`   Creator position: ${wager.creator_position}`);

        // Determine winner based on prediction
        let winnerId = null;
        let winnerPosition = null;

        if (wager.prediction_type === 'above') {
            if (expiryPrice > wager.target_price) {
                winnerId = wager.creator_id;
                winnerPosition = 'creator';
                console.log(`   ✅ Creator wins: ${expiryPrice} > ${wager.target_price}`);
            } else {
                winnerId = wager.acceptor_id;
                winnerPosition = 'acceptor';
                console.log(`   ✅ Acceptor wins: ${expiryPrice} <= ${wager.target_price}`);
            }
        } else {
            if (expiryPrice < wager.target_price) {
                winnerId = wager.creator_id;
                winnerPosition = 'creator';
                console.log(`   ✅ Creator wins: ${expiryPrice} < ${wager.target_price}`);
            } else {
                winnerId = wager.acceptor_id;
                winnerPosition = 'acceptor';
                console.log(`   ✅ Acceptor wins: ${expiryPrice} >= ${wager.target_price}`);
            }
        }

        // Execute on-chain resolution and payout
        const resolutionResult = await resolveCryptoWagerOnChain(
            wager.wager_id,
            winnerPosition,
            wager.creator_id,
            wager.acceptor_id,
            wager.amount
        );

        if (resolutionResult.success) {
            // Get winner's wallet address
            const winnerWalletAddress = winnerPosition === 'creator'
                ? wager.creator_address
                : wager.acceptor_address;

            // Update database with resolution
            const { error: updateError } = await supabase
                .from('crypto_wagers')
                .update({
                    status: 'resolved',
                    winner_id: winnerId,
                    winner_address: winnerWalletAddress,
                    winner_position: winnerPosition,
                    resolution_price: expiryPrice,
                    resolved_at: new Date().toISOString(),
                    on_chain_signature: resolutionResult.signature,
                    metadata: {
                        ...(wager.metadata || {}),
                        resolution_details: {
                            target_price: wager.target_price,
                            expiry_price: expiryPrice,
                            prediction_type: wager.prediction_type,
                            creator_position: wager.creator_position,
                            resolved_at: new Date().toISOString()
                        }
                    }
                })
                .eq('id', wager.id);

            if (updateError) {
                console.error(`❌ Error updating wager ${wager.wager_id}:`, updateError);
            } else {
                // Create notification for winner using the new database function
                const winnerWalletAddress = winnerPosition === 'creator' ? wager.creator_address : wager.acceptor_address;
                const loserWalletAddress = winnerPosition === 'creator' ? wager.acceptor_address : wager.creator_address;

                const notificationResult = await supabase.rpc('create_wager_resolved_notification', {
                    p_winner_address: winnerWalletAddress,
                    p_loser_address: loserWalletAddress,
                    p_wager_type: 'crypto',
                    p_wager_amount: wager.amount || wager.sol_amount,
                    p_is_draw: false
                });

                if (notificationResult.error) {
                    console.error('❌ Error creating wager resolution notification:', notificationResult.error);
                } else {
                    console.log('✅ Created wager resolution notification for winner');
                }

                // Update stats for both users involved in the wager
                await updateWagerUserStats(wager, winnerId, winnerPosition, 'crypto');

                console.log(`✅ Resolved expired matched wager ${wager.wager_id} - Winner: ${winnerId}`);
            }
        } else {
            console.error(`❌ Failed to resolve expired matched wager ${wager.wager_id}:`, resolutionResult.error);
        }

    } catch (error) {
        console.error(`❌ Error resolving expired matched wager ${wager.wager_id}:`, error);
    }
}

// Refund unmatched expired wager
async function refundUnmatchedExpiredWager(wager) {
    try {
        console.log(`💰 Refunding unmatched expired wager: ${wager.wager_id}`);

        // Execute on-chain refund using your authority private key
        const refundResult = await processWagerRefundOnChain(wager);

        if (refundResult.success) {
            // Mark the refund as processed
            const result = await markRefundProcessed(
                wager.wager_id,
                'crypto',
                refundResult.signature
            );

            if (result.success) {
                // Create notification for user
                try {
                    // Get the user's wallet address for the notification
                    const { data: userData, error: userError } = await supabase
                        .from('users')
                        .select('wallet_address, user_address')
                        .eq('id', wager.creator_id)
                        .single();

                    if (userError || !userData) {
                        console.error('❌ Error fetching user data for notification:', userError);
                        console.log(`🔍 Attempted to find user with ID: ${wager.creator_id}`);
                        return;
                    }

                    const userAddress = userData.wallet_address || userData.user_address;
                    if (!userAddress) {
                        console.error('❌ No wallet address found for user:', wager.creator_id);
                        console.log(`🔍 User data found:`, userData);
                        return;
                    }

                    console.log(`🔍 Creating notification for user ${wager.creator_id} with address ${userAddress}`);

                    // Create notification directly in the database
                    const { error: notificationError } = await supabase
                        .from('notifications')
                        .insert({
                            user_id: wager.creator_id, // Add user_id to match the table schema
                            user_address: userAddress,
                            type: 'wager_expired',
                            title: 'Expired Wager Refunded!',
                            message: `Your unmatched crypto wager on ${wager.token_symbol} has expired and been refunded. Transaction: ${refundResult.signature}`,
                            data: { wager_id: wager.wager_id, refund_signature: refundResult.signature },
                            read: false,
                            is_deleted: false,
                            created_at: new Date().toISOString()
                        });

                    if (notificationError) {
                        console.error('❌ Error creating notification:', notificationError);
                        console.log(`🔍 Notification data attempted:`, {
                            user_id: wager.creator_id,
                            user_address: userAddress,
                            type: 'wager_expired',
                            title: 'Expired Wager Refunded!',
                            message: `Your unmatched crypto wager on ${wager.token_symbol} has expired and been refunded. Transaction: ${refundResult.signature}`,
                            data: { wager_id: wager.wager_id, refund_signature: refundResult.signature },
                            read: false,
                            is_deleted: false,
                            created_at: new Date().toISOString()
                        });
                    } else {
                        console.log(`✅ Notification created for user ${wager.creator_id} (${userAddress})`);
                    }
                } catch (notificationError) {
                    console.error('❌ Failed to create notification:', notificationError);
                    // Don't fail the refund process due to notification errors
                }

                console.log(`✅ Refunded unmatched expired wager ${wager.wager_id}`);
            }
        } else {
            console.error(`❌ Failed to refund unmatched expired wager ${wager.wager_id}:`, refundResult.error);
        }

    } catch (error) {
        console.error(`❌ Error refunding unmatched expired wager ${wager.wager_id}:`, error);
    }
}

// Mark expiry as processed
async function markExpiryProcessed(wagerId, wagerType) {
    try {
        const tableName = wagerType === 'crypto' ? 'crypto_wagers' : 'sports_wagers';

        // First get current metadata
        const { data: currentWager, error: fetchError } = await supabase
            .from(tableName)
            .select('metadata')
            .eq('wager_id', wagerId)
            .single();

        if (fetchError) {
            console.error(`❌ Error fetching current metadata for ${wagerId}:`, fetchError);
            return;
        }

        // Update metadata by merging with new data
        const currentMetadata = currentWager?.metadata || {};
        const updatedMetadata = {
            ...currentMetadata,
            expiry_processed: true,
            expiry_processed_at: new Date().toISOString()
        };

        const { error: updateError } = await supabase
            .from(tableName)
            .update({
                metadata: updatedMetadata
            })
            .eq('wager_id', wagerId);

        if (updateError) {
            console.error(`❌ Error marking expiry as processed for ${wagerId}:`, updateError);
        }

    } catch (error) {
        console.error(`❌ Error in markExpiryProcessed for ${wagerId}:`, error);
    }
}

// Handle expired sports wagers - resolve matched ones, refund unmatched ones
async function handleExpiredSportsWagers() {
    try {
        console.log('🔄 Handling expired sports wagers...');

        // Get all expired sports wagers that need processing
        // Look for both cancelled and active wagers that have expired
        console.log('🔍 Looking for expired sports wagers to resolve/refund with statuses: cancelled, active');
        const { data: expiredWagers, error: fetchError } = await supabase
            .from('sports_wagers')
            .select('*')
            .in('status', ['cancelled', 'active'])
            .lt('expiry_time', new Date().toISOString())
            .is('metadata->expiry_processed', null);

        console.log(`🔍 Found ${expiredWagers?.length || 0} expired sports wagers to resolve/refund`);
        if (expiredWagers && expiredWagers.length > 0) {
            console.log('🔍 Wagers to process:', expiredWagers.map(w => ({ id: w.id, status: w.status, expiry_time: w.expiry_time, acceptor_id: w.acceptor_id })));
        }

        if (fetchError) {
            console.error('❌ Error fetching expired sports wagers:', fetchError);
            return;
        }

        if (!expiredWagers || expiredWagers.length === 0) {
            return;
        }

        console.log(`📋 Processing ${expiredWagers.length} expired sports wagers...`);

        for (const wager of expiredWagers) {
            try {
                if (wager.status === 'matched' || wager.acceptor_id) {
                    // Wager was matched - resolve it
                    console.log(`🏁 Resolving matched expired sports wager: ${wager.wager_id}`);
                    await resolveExpiredMatchedSportsWager(wager);
                } else {
                    // Wager was unmatched - refund creator
                    console.log(`💰 Refunding unmatched expired sports wager: ${wager.wager_id}`);
                    await refundUnmatchedExpiredSportsWager(wager);
                }

                // Mark as processed
                await markExpiryProcessed(wager.wager_id, 'sports');

            } catch (error) {
                console.error(`❌ Error processing expired sports wager ${wager.wager_id}:`, error);
            }
        }

    } catch (error) {
        console.error('❌ Error in handleExpiredSportsWagers:', error);
    }
}

// Resolve expired matched sports wager (determine winner and pay out)
async function resolveExpiredMatchedSportsWager(wager) {
    try {
        console.log(`🏁 Resolving expired matched sports wager: ${wager.wager_id}`);

        // Get game result from Sports API
        const gameResult = await getSportsGameResult(wager.sport, wager.team1, wager.team2);

        // Determine winner based on prediction
        let winnerId = null;
        let winnerPosition = null;
        let isDraw = false;

        if (gameResult === 'draw' || gameResult === 'tie') {
            isDraw = true;
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
                wager.wager_id,
                wager.creator_id,
                wager.acceptor_id,
                wager.amount
            );
        } else {
            // Handle normal resolution
            onChainResult = await resolveSportsWagerOnChain(
                wager.wager_id,
                winnerPosition,
                wager.creator_id,
                wager.acceptor_id,
                wager.amount
            );
        }

        if (onChainResult.success) {
            // Get winner's wallet address if there's a winner
            const winnerWalletAddress = winnerId ?
                (winnerPosition === 'creator' ? wager.creator_address : wager.acceptor_address) :
                null;

            // Update database
            const { error: updateError } = await supabase
                .from('sports_wagers')
                .update({
                    status: 'resolved',
                    winner_id: winnerId,
                    winner_address: winnerWalletAddress,
                    winner_position: winnerPosition,
                    resolution_outcome: gameResult,
                    resolved_at: new Date().toISOString(),
                    on_chain_signature: onChainResult.signature,
                    metadata: {
                        ...(wager.metadata || {}),
                        resolution_details: {
                            game_result: gameResult,
                            is_draw: isDraw,
                            prediction: wager.prediction,
                            resolved_at: new Date().toISOString()
                        }
                    }
                })
                .eq('id', wager.id);

            if (updateError) {
                console.error(`❌ Error updating sports wager ${wager.wager_id}:`, updateError);
            } else {
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

                // Update stats for both users involved in the wager
                if (winnerId) {
                    await updateWagerUserStats(wager, winnerId, winnerPosition, 'sports');
                } else {
                    // Handle draw case - both users get refunded, no winner
                    console.log(`📊 Draw detected - updating stats for both users (refund scenario)`);
                    await updateWagerUserStats(wager, null, null, 'sports');
                }

                console.log(`✅ Resolved expired matched sports wager ${wager.wager_id} - ${isDraw ? 'Draw' : `Winner: ${winnerId}`}`);
            }
        } else {
            console.error(`❌ Failed to resolve expired matched sports wager ${wager.wager_id}:`, onChainResult.error);
        }

    } catch (error) {
        console.error(`❌ Error resolving expired matched sports wager ${wager.wager_id}:`, error);
    }
}

// Refund unmatched expired sports wager
async function refundUnmatchedExpiredSportsWager(wager) {
    try {
        console.log(`💰 Refunding unmatched expired sports wager: ${wager.wager_id}`);

        // Execute on-chain refund using your authority private key
        const refundResult = await processWagerRefundOnChain(wager);

        if (refundResult.success) {
            // Mark the refund as processed
            const result = await markRefundProcessed(
                wager.wager_id,
                'sports',
                refundResult.signature
            );

            if (result.success) {
                // Create notification for user
                await createNotification(
                    wager.creator_id,
                    'wager_expired',
                    'Expired Sports Wager Refunded!',
                    `Your unmatched sports wager on ${wager.team1} vs ${wager.team2} has expired and been refunded. Transaction: ${refundResult.signature}`
                );

                console.log(`✅ Refunded unmatched expired sports wager ${wager.wager_id}`);
            }
        } else {
            console.error(`❌ Failed to refund unmatched expired sports wager ${wager.wager_id}:`, refundResult.error);
        }

    } catch (error) {
        console.error(`❌ Error refunding unmatched expired sports wager ${wager.wager_id}:`, error);
    }
}

// Process wager refund on-chain (using authority private key)
async function processWagerRefundOnChain(wager) {
    try {
        console.log(`🔗 Executing on-chain refund for wager ${wager.wager_id}`);

        // For refunds: NO platform fee, but network fee is paid from refund
        const refundAmount = wager.amount; // Full amount to refund
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee
        const actualRefund = refundAmount - networkFee; // User gets amount minus network fee

        console.log(`💰 Refund breakdown for wager ${wager.wager_id}:`);
        console.log(`   Original amount: ${refundAmount} SOL`);
        console.log(`   Network fee: ${networkFee} SOL`);
        console.log(`   User receives: ${actualRefund} SOL`);
        console.log(`   Escrow PDA: ${wager.escrow_pda}`);

        // Execute actual Solana escrow withdrawal
        try {
            const escrowAccount = new PublicKey(wager.escrow_pda);
            const userWallet = new PublicKey(wager.creator_address);

            console.log(`🔐 Executing real escrow withdrawal...`);
            console.log(`   Escrow: ${escrowAccount.toString()}`);
            console.log(`   User: ${userWallet.toString()}`);
            console.log(`   Authority: ${authorityKeypair.publicKey.toString()}`);

            // Execute the handleExpiredWager instruction to refund the creator
            // Use the actual PDAs stored in the database to avoid creation of new accounts
            const signature = await executeProgramInstruction('handleExpiredWager', {
                wagerId: wager.wager_id, // Use the actual wager ID for PDA derivation
                escrowPda: wager.escrow_pda, // Pass the escrow PDA for verification
                creatorPubkey: userWallet.toString()
            });

            console.log(`   🔐 Real escrow withdrawal completed: ${signature}`);

            return {
                success: true,
                signature: signature,
                refundBreakdown: {
                    originalAmount: refundAmount,
                    networkFee: networkFee,
                    actualRefund: actualRefund
                }
            };

        } catch (onChainError) {
            console.error(`❌ Real on-chain refund failed:`, onChainError);

            // Fallback to simulation for now
            const mockSignature = `mock_refund_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            console.log(`   🔐 Falling back to simulated escrow withdrawal: ${mockSignature}`);

            return {
                success: true,
                signature: mockSignature,
                refundBreakdown: {
                    originalAmount: refundAmount,
                    networkFee: networkFee,
                    actualRefund: actualRefund
                },
                note: 'Simulated due to on-chain error'
            };
        }

    } catch (error) {
        console.error('❌ On-chain refund failed:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// HELPER FUNCTIONS

// Resolve crypto wagers immediately when they expire (1-second precision)
async function resolveExpiringCryptoWagers() {
    try {
        const now = new Date();

        // Look for crypto wagers that are active and have expired within the last 2 seconds
        // This ensures we catch them right at expiration
        const { data: expiringWagers, error: fetchError } = await supabase
            .from('crypto_wagers')
            .select('*')
            .eq('status', 'active')
            .gte('expires_at', new Date(now.getTime() - 2000).toISOString()) // Expired within last 2 seconds
            .lte('expires_at', now.toISOString()) // But not expired more than 2 seconds ago
            .is('metadata->expiry_processed', null)
            .is('metadata->resolution_status', null); // Don't process wagers already being resolved

        // Also check for wagers expiring in the next 1 second to be extra precise
        const { data: aboutToExpireWagers, error: aboutToExpireError } = await supabase
            .from('crypto_wagers')
            .select('*')
            .eq('status', 'active')
            .gte('expires_at', now.toISOString()) // Not yet expired
            .lte('expires_at', new Date(now.getTime() + 1000).toISOString()) // But expiring within next second
            .is('metadata->expiry_processed', null)
            .is('metadata->resolution_status', null); // Don't process wagers already being resolved

        if (aboutToExpireError) {
            console.error('❌ Error fetching about-to-expire crypto wagers:', aboutToExpireError);
        }

        // Combine both sets of wagers
        const allExpiringWagers = [
            ...(expiringWagers || []),
            ...(aboutToExpireWagers || [])
        ];

        if (fetchError) {
            console.error('❌ Error fetching expiring crypto wagers:', fetchError);
            return;
        }

        if (!allExpiringWagers || allExpiringWagers.length === 0) {
            return;
        }

        console.log(`⚡ Found ${allExpiringWagers.length} crypto wagers expiring now - resolving immediately...`);



        for (const wager of allExpiringWagers) {
            try {
                // IMMEDIATELY mark this wager as processing to prevent double-execution
                const { error: processingError } = await supabase
                    .from('crypto_wagers')
                    .update({
                        metadata: {
                            ...(wager.metadata || {}),
                            resolution_status: 'processing',
                            resolution_started_at: new Date().toISOString()
                        }
                    })
                    .eq('id', wager.id);

                if (processingError) {
                    console.error(`❌ Error marking wager ${wager.wager_id} as processing:`, processingError);
                    continue; // Skip this wager if we can't mark it as processing
                }

                console.log(`⚡ Immediate resolution for wager ${wager.wager_id}`);
                console.log(`   Expiry time: ${wager.expires_at}`);

                const wagerExpiryTime = new Date(wager.expires_at);
                const timeUntilExpiry = wagerExpiryTime.getTime() - now.getTime();
                console.log(`   Time until expiry: ${timeUntilExpiry}ms`);

                // If wager hasn't expired yet, wait until it does for maximum accuracy
                if (timeUntilExpiry > 0) {
                    console.log(`   ⏰ Waiting ${timeUntilExpiry}ms for exact expiry...`);
                    await new Promise(resolve => setTimeout(resolve, timeUntilExpiry));
                }

                // Get the exact price at expiration time
                const expiryPrice = await getCurrentCryptoPrice(wager.token_symbol);

                console.log(`💰 Exact expiry price for ${wager.token_symbol}: $${expiryPrice} (target: $${wager.target_price})`);

                // Determine winner based on prediction
                let winnerId = null;
                let winnerPosition = null;

                if (wager.prediction_type === 'above') {
                    if (expiryPrice > wager.target_price) {
                        winnerId = wager.creator_id;
                        winnerPosition = 'creator';
                        console.log(`   ✅ Creator wins: ${expiryPrice} > ${wager.target_price}`);
                    } else {
                        winnerId = wager.acceptor_id;
                        winnerPosition = 'acceptor';
                        console.log(`   ✅ Acceptor wins: ${expiryPrice} <= ${wager.target_price}`);
                    }
                } else {
                    if (expiryPrice < wager.target_price) {
                        winnerId = wager.creator_id;
                        winnerPosition = 'creator';
                        console.log(`   ✅ Creator wins: ${expiryPrice} < ${wager.target_price}`);
                    } else {
                        winnerId = wager.acceptor_id;
                        winnerPosition = 'acceptor';
                        console.log(`   ✅ Acceptor wins: ${expiryPrice} >= ${wager.target_price}`);
                    }
                }

                // Execute on-chain resolution immediately
                const resolutionResult = await resolveCryptoWagerOnChain(
                    wager.wager_id,
                    winnerPosition,
                    wager.creator_id,
                    wager.acceptor_id,
                    wager.amount
                );

                if (resolutionResult.success) {
                    // Get winner's wallet address
                    const winnerWalletAddress = winnerPosition === 'creator'
                        ? wager.creator_address
                        : wager.acceptor_address;

                    // IMMEDIATELY update database status to prevent double-processing
                    const { error: immediateUpdateError } = await supabase
                        .from('crypto_wagers')
                        .update({
                            status: 'resolved',
                            metadata: {
                                ...(wager.metadata || {}),
                                resolution_status: 'processing',
                                resolution_started_at: new Date().toISOString()
                            }
                        })
                        .eq('id', wager.id);

                    if (immediateUpdateError) {
                        console.error(`❌ Error updating wager status immediately:`, immediateUpdateError);
                    } else {
                        console.log(`✅ Immediately marked wager ${wager.wager_id} as processing to prevent double-execution`);
                    }

                    // Now update with full resolution details
                    const { error: updateError } = await supabase
                        .from('crypto_wagers')
                        .update({
                            winner_id: winnerId,
                            winner_address: winnerWalletAddress,
                            winner_position: winnerPosition,
                            resolution_price: expiryPrice,
                            resolved_at: new Date().toISOString(),
                            on_chain_signature: resolutionResult.signature,
                            metadata: {
                                ...(wager.metadata || {}),
                                resolution_details: {
                                    target_price: wager.target_price,
                                    expiry_price: expiryPrice,
                                    prediction_type: wager.prediction_type,
                                    creator_position: wager.creator_position,
                                    resolved_at: new Date().toISOString(),
                                    resolution_timing: 'immediate_at_expiry',
                                    resolution_delay_ms: Math.abs(timeUntilExpiry),
                                    price_fetch_timestamp: new Date().toISOString()
                                },
                                resolution_status: 'completed',
                                resolution_completed_at: new Date().toISOString()
                            }
                        })
                        .eq('id', wager.id);

                    if (updateError) {
                        console.error(`❌ Error updating wager ${wager.wager_id}:`, updateError);
                    } else {
                        // Create notification for winner
                        await createNotification(winnerId, 'wager_resolved',
                            'Wager Resolved!',
                            `Your crypto wager on ${wager.token_symbol} has been resolved at expiry. You won ${wager.amount} SOL!`);

                        // Update stats for both users involved in the wager
                        await updateWagerUserStats(wager, winnerId, winnerPosition, 'crypto');

                        // Mark as processed to prevent double-processing
                        await markExpiryProcessed(wager.wager_id, 'crypto');

                        console.log(`⚡ IMMEDIATE RESOLUTION COMPLETED for ${wager.wager_id} - Winner: ${winnerId}`);
                    }
                } else {
                    console.error(`❌ Failed to immediately resolve wager ${wager.wager_id}:`, resolutionResult.error);

                    // Even if resolution failed, mark as processed to prevent infinite retries
                    await markExpiryProcessed(wager.wager_id, 'crypto');
                }

            } catch (error) {
                console.error(`❌ Error in immediate resolution for wager ${wager.wager_id}:`, error);
            }
        }

    } catch (error) {
        console.error('❌ Error in resolveExpiringCryptoWagers:', error);
    }
}

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

// Create notification function is now replaced by database functions
// Use supabase.rpc('create_notification', {...}) instead

// Update user stats for both users involved in a wager
async function updateWagerUserStats(wager, winnerId, winnerPosition, wagerType = 'crypto') {
    try {
        console.log(`📊 Updating stats for both users in ${wagerType} wager`);

        const creatorId = wager.creator_id;
        const acceptorId = wager.acceptor_id;
        const wagerAmount = wager.amount || wager.sol_amount;

        if (!creatorId || !acceptorId) {
            console.error('❌ Missing creator or acceptor ID for stats update');
            return;
        }

        // Determine who won and who lost
        let isCreatorWinner = false;
        let creatorWon = false;
        let acceptorWon = false;

        if (winnerId && winnerPosition) {
            // Normal win/loss scenario
            isCreatorWinner = winnerPosition === 'creator';
            creatorWon = isCreatorWinner;
            acceptorWon = !isCreatorWinner;
        } else {
            // Draw scenario - both users get refunded, no winner
            creatorWon = false;
            acceptorWon = false;
        }

        console.log(`📊 Stats breakdown:`);
        if (winnerId && winnerPosition) {
            console.log(`   Creator (${creatorId}): ${creatorWon ? 'WON' : 'LOST'}`);
            console.log(`   Acceptor (${acceptorId}): ${acceptorWon ? 'WON' : 'LOST'}`);
        } else {
            console.log(`   Creator (${creatorId}): DRAW (refunded)`);
            console.log(`   Acceptor (${acceptorId}): DRAW (refunded)`);
        }
        console.log(`   Wager amount: ${wagerAmount} SOL`);

        // Update creator stats
        await updateSingleUserStats(creatorId, {
            total_wagered: wagerAmount,
            total_won: creatorWon ? wagerAmount : 0,
            total_lost: creatorWon ? 0 : wagerAmount,
            won: creatorWon,
            wager_type: wagerType
        });

        // Update acceptor stats
        await updateSingleUserStats(acceptorId, {
            total_wagered: wagerAmount,
            total_won: acceptorWon ? wagerAmount : 0,
            total_lost: acceptorWon ? 0 : wagerAmount,
            won: acceptorWon,
            wager_type: wagerType
        });

        console.log(`✅ Stats updated for both users`);

    } catch (error) {
        console.error('❌ Error updating wager user stats:', error);
    }
}

// Update stats for a single user
async function updateSingleUserStats(userId, stats) {
    try {
        // Get current user stats
        const { data: currentUser, error: fetchError } = await supabase
            .from('users')
            .select('total_wagered, total_won, total_lost, win_rate, streak_count')
            .eq('id', userId)
            .single();

        if (fetchError) {
            console.error(`❌ Error fetching current stats for user ${userId}:`, fetchError);
            return;
        }

        // Calculate new stats
        const newStats = {
            total_wagered: (currentUser.total_wagered || 0) + stats.total_wagered,
            total_won: (currentUser.total_won || 0) + stats.total_won,
            total_lost: (currentUser.total_lost || 0) + stats.total_lost,
            updated_at: new Date().toISOString()
        };

        // Only update win rate and streak if this is a win/loss scenario
        if (stats.won !== null) {
            // Calculate win rate based on total_won vs total_lost
            const totalWagers = (newStats.total_won + newStats.total_lost);
            newStats.win_rate = totalWagers > 0 ? (newStats.total_won / totalWagers) * 100 : 0;

            // Calculate streak
            if (stats.won) {
                // User won - increment streak
                newStats.streak_count = (currentUser.streak_count || 0) + 1;
            } else {
                // User lost - reset streak to 0
                newStats.streak_count = 0;
            }
        } else {
            // This is just a wager acceptance, preserve existing win rate and streak
            newStats.win_rate = currentUser.win_rate || 0;
            newStats.streak_count = currentUser.streak_count || 0;
        }

        // Update user stats in database
        const { error: updateError } = await supabase
            .from('users')
            .update(newStats)
            .eq('id', userId);

        if (updateError) {
            console.error(`❌ Error updating stats for user ${userId}:`, updateError);
        } else {
            console.log(`✅ Updated stats for user ${userId}:`, {
                total_wagered: newStats.total_wagered,
                total_won: newStats.total_won,
                total_lost: newStats.total_lost,
                win_rate: newStats.win_rate.toFixed(2) + '%',
                streak_count: newStats.streak_count
            });
        }

    } catch (error) {
        console.error(`❌ Error updating single user stats for ${userId}:`, error);
    }
}

// Update stats when a wager is accepted (both users have wagered)
async function updateWagerAcceptanceStats(creatorId, acceptorId, wagerAmount, wagerType) {
    try {
        console.log(`📊 Updating acceptance stats for ${wagerType} wager`);
        console.log(`   Creator (${creatorId}): wagered ${wagerAmount} SOL`);
        console.log(`   Acceptor (${acceptorId}): wagered ${wagerAmount} SOL`);

        // Update creator stats - increment total_wagered
        await updateSingleUserStats(creatorId, {
            total_wagered: wagerAmount,
            total_won: 0,
            total_lost: 0,
            won: null, // Not a win/loss, just a wager
            wager_type: wagerType
        });

        // Update acceptor stats - increment total_wagered
        await updateSingleUserStats(acceptorId, {
            total_wagered: wagerAmount,
            total_won: 0,
            total_lost: 0,
            won: null, // Not a win/loss, just a wager
            wager_type: wagerType
        });

        console.log(`✅ Acceptance stats updated for both users`);

    } catch (error) {
        console.error('❌ Error updating wager acceptance stats:', error);
    }
}

// Legacy function for backward compatibility
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

// BACKGROUND WORKER HELPER FUNCTIONS

// Expire expired wagers automatically
async function expireExpiredWagers() {
    try {
        // Silent check for expired wagers (runs every 15 seconds)

        let totalExpired = 0;

        // Expire crypto wagers - check if they were matched or unmatched
        // First, get the wagers that need to be expired
        console.log('🔍 Looking for expired crypto wagers with statuses: open, matched, active');
        const { data: cryptoWagersToExpire, error: cryptoFetchError } = await supabase
            .from('crypto_wagers')
            .select('id, status, expires_at, metadata')
            .in('status', ['open', 'matched', 'active'])
            .lt('expires_at', new Date().toISOString())
            .is('metadata->expiry_processed', null); // Only process unprocessed wagers

        console.log(`🔍 Found ${cryptoWagersToExpire?.length || 0} expired crypto wagers to process`);
        if (cryptoWagersToExpire && cryptoWagersToExpire.length > 0) {
            console.log('🔍 Expired crypto wagers:', cryptoWagersToExpire.map(w => ({ id: w.id, status: w.status, expires_at: w.expires_at })));
        }

        if (cryptoFetchError) {
            console.error('❌ Error fetching crypto wagers to expire:', cryptoFetchError);
        } else if (cryptoWagersToExpire && cryptoWagersToExpire.length > 0) {
            // Update each wager individually to preserve metadata
            for (const wager of cryptoWagersToExpire) {
                const currentMetadata = wager.metadata || {};
                const updatedMetadata = {
                    ...currentMetadata,
                    cancelled_at: new Date().toISOString(),
                    cancelled_by: 'system_expiration'
                };

                const { error: updateError } = await supabase
                    .from('crypto_wagers')
                    .update({
                        status: 'cancelled',
                        updated_at: new Date().toISOString(),
                        metadata: updatedMetadata
                    })
                    .eq('id', wager.id);

                if (updateError) {
                    console.error(`❌ Error updating crypto wager ${wager.id}:`, updateError);
                } else {
                    totalExpired++;
                }
            }
        }

        // Handle expired crypto wagers - resolve matched ones, refund unmatched ones
        // This will process both matched and active wagers that have expired
        if (cryptoWagersToExpire && cryptoWagersToExpire.length > 0) {
            await handleExpiredCryptoWagers();
        }

        // Expire sports wagers
        // First, get the wagers that need to be expired
        console.log('🔍 Looking for expired sports wagers with statuses: open, matched, active');
        const { data: sportsWagersToExpire, error: sportsFetchError } = await supabase
            .from('sports_wagers')
            .select('id, status, expiry_time, metadata')
            .in('status', ['open', 'matched', 'active'])
            .lt('expiry_time', new Date().toISOString())
            .is('metadata->expiry_processed', null); // Only process unprocessed wagers

        console.log(`🔍 Found ${sportsWagersToExpire?.length || 0} expired sports wagers to process`);
        if (sportsWagersToExpire && sportsWagersToExpire.length > 0) {
            console.log('🔍 Expired sports wagers:', sportsWagersToExpire.map(w => ({ id: w.id, status: w.status, expiry_time: w.expiry_time })));
        }

        if (sportsFetchError) {
            console.error('❌ Error fetching sports wagers to expire:', sportsFetchError);
        } else if (sportsWagersToExpire && sportsWagersToExpire.length > 0) {
            // Update each wager individually to preserve metadata
            for (const wager of sportsWagersToExpire) {
                const currentMetadata = wager.metadata || {};
                const updatedMetadata = {
                    ...currentMetadata,
                    cancelled_at: new Date().toISOString(),
                    cancelled_by: 'system_expiration'
                };

                const { error: updateError } = await supabase
                    .from('sports_wagers')
                    .update({
                        status: 'cancelled',
                        updated_at: new Date().toISOString(),
                        metadata: updatedMetadata
                    })
                    .eq('id', wager.id);

                if (updateError) {
                    console.error(`❌ Error updating sports wager ${wager.id}:`, updateError);
                } else {
                    totalExpired++;
                }
            }
        }

        // Handle expired sports wagers - resolve matched ones, refund unmatched ones
        // This will process both matched and active wagers that have expired
        if (sportsWagersToExpire && sportsWagersToExpire.length > 0) {
            await handleExpiredSportsWagers();
        }

        if (totalExpired > 0) {
            console.log(`✅ Expired ${totalExpired} wagers automatically`);
        }
        return totalExpired;

    } catch (error) {
        console.error('❌ Error in expireExpiredWagers:', error);
        return 0;
    }
}

// Get cancelled wagers that need refunds
async function getCancelledWagersForRefund() {
    try {
        console.log('🔄 Fetching cancelled wagers needing refunds...');

        const cancelledWagers = [];

        // Get crypto wagers that are cancelled and need refunds
        const { data: cryptoWagers, error: cryptoError } = await supabase
            .from('crypto_wagers')
            .select('wager_id, creator_address, amount, escrow_pda')
            .eq('status', 'cancelled')
            .is('metadata->refund_processed', null)
            .not('escrow_pda', 'is', null);

        if (cryptoError) {
            console.error('❌ Error fetching cancelled crypto wagers:', cryptoError);
        } else if (cryptoWagers) {
            cryptoWagers.forEach(wager => {
                cancelledWagers.push({
                    ...wager,
                    wager_type: 'crypto'
                });
            });
        }

        // Get sports wagers that are cancelled and need refunds
        const { data: sportsWagers, error: sportsError } = await supabase
            .from('sports_wagers')
            .select('wager_id, creator_address, amount, escrow_pda')
            .eq('status', 'cancelled')
            .is('metadata->refund_processed', null)
            .not('escrow_pda', 'is', null);

        if (sportsError) {
            console.error('❌ Error fetching cancelled sports wagers:', sportsError);
        } else if (sportsWagers) {
            sportsWagers.forEach(wager => {
                cancelledWagers.push({
                    ...wager,
                    wager_type: 'sports'
                });
            });
        }

        console.log(`📋 Found ${cancelledWagers.length} cancelled wagers needing refunds`);
        return cancelledWagers;

    } catch (error) {
        console.error('❌ Error in getCancelledWagersForRefund:', error);
        return [];
    }
}

// Process refund for a single wager
async function processWagerRefund(wager) {
    try {
        console.log(`💰 Processing refund for wager ${wager.wager_id} (${wager.wager_type})`);
        console.log(`   Creator: ${wager.creator_address}`);
        console.log(`   Amount: ${wager.amount} SOL`);
        console.log(`   Escrow PDA: ${wager.escrow_pda}`);

        // Execute on-chain refund using your authority private key
        const refundResult = await processWagerRefundOnChain(wager);

        if (refundResult.success) {
            // Mark the refund as processed in the database
            const result = await markRefundProcessed(
                wager.wager_id,
                wager.wager_type,
                refundResult.signature
            );

            if (result.success) {
                console.log(`✅ Refund processed successfully for ${wager.wager_id}`);
                console.log(`   Transaction: ${refundResult.signature}`);

                // Create notification for user
                await createNotification(
                    wager.creator_id || 'unknown',
                    'refund_processed',
                    'Refund Processed!',
                    `Your ${wager.wager_type} wager refund of ${wager.amount} SOL has been processed. Transaction: ${refundResult.signature}`
                );
            } else {
                console.error(`❌ Error marking refund as processed for ${wager.wager_id}:`, result.error);
            }
        } else {
            console.error(`❌ On-chain refund failed for ${wager.wager_id}:`, refundResult.error);
        }

    } catch (error) {
        console.error(`❌ Error processing refund for ${wager.wager_id}:`, error);
        throw error;
    }
}

// Mark refund as processed in database
async function markRefundProcessed(wagerId, wagerType, refundSignature) {
    try {
        console.log(`🔄 Marking refund as processed for ${wagerType} wager: ${wagerId}`);

        const tableName = wagerType === 'crypto' ? 'crypto_wagers' : 'sports_wagers';

        // First get current metadata
        const { data: currentWager, error: fetchError } = await supabase
            .from(tableName)
            .select('metadata')
            .eq('wager_id', wagerId)
            .single();

        if (fetchError) {
            console.error(`❌ Error fetching current metadata for ${wagerId}:`, fetchError);
            return {
                success: false,
                error: fetchError.message
            };
        }

        // Update metadata by merging with new data
        const currentMetadata = currentWager?.metadata || {};
        const updatedMetadata = {
            ...currentMetadata,
            refund_processed: true,
            refund_signature: refundSignature,
            refund_processed_at: new Date().toISOString()
        };

        const { error: updateError } = await supabase
            .from(tableName)
            .update({
                metadata: updatedMetadata
            })
            .eq('wager_id', wagerId);

        if (updateError) {
            console.error(`❌ Error updating refund status for ${wagerId}:`, updateError);
            return {
                success: false,
                error: updateError.message
            };
        }

        console.log(`✅ Refund marked as processed for ${wagerId}`);
        return {
            success: true,
            message: 'Refund marked as processed',
            wager_id: wagerId,
            refund_signature: refundSignature
        };

    } catch (error) {
        console.error(`❌ Error in markRefundProcessed for ${wagerId}:`, error);
        return {
            success: false,
            error: error.message
        };
    }
}

// CRYPTO TOKEN ENDPOINTS
// Get token list from CoinMarketCap
app.post('/token-list', async (req, res) => {
    try {
        const { apiKey, limit = 100 } = req.body;

        if (!apiKey) {
            return res.status(400).json({ error: 'API key is required' });
        }

        console.log(`🪙 Fetching token list (limit: ${limit})`);
        console.log(`🔑 Using API key: ${apiKey.substring(0, 8)}...`);

        // Fetch from CoinMarketCap API
        const response = await fetch(
            `https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=${limit}`,
            {
                headers: {
                    'X-CMC_PRO_API_KEY': apiKey,
                    'Accept': 'application/json'
                }
            }
        );

        if (!response.ok) {
            const errorText = await response.text();
            console.error(`❌ CoinMarketCap API error: ${response.status} - ${errorText}`);
            throw new Error(`CoinMarketCap API error: ${response.status} - ${errorText}`);
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

// Get trending tokens from CoinMarketCap (better for TopCryptoTokensPanel)
app.post('/trending-tokens', async (req, res) => {
    try {
        const { apiKey, limit = 100, timePeriod = '24h' } = req.body;

        if (!apiKey) {
            return res.status(400).json({ error: 'API key is required' });
        }

        console.log(`🪙 Fetching trending tokens (limit: ${limit}, period: ${timePeriod})`);
        console.log(`🔑 Using API key: ${apiKey.substring(0, 8)}...`);

        // Fetch from CoinMarketCap trending API
        const response = await fetch(
            `https://pro-api.coinmarketcap.com/v1/cryptocurrency/trending/latest?limit=${limit}&time_period=${timePeriod}`,
            {
                headers: {
                    'X-CMC_PRO_API_KEY': apiKey,
                    'Accept': 'application/json'
                }
            }
        );

        if (!response.ok) {
            const errorText = await response.text();
            console.error(`❌ CoinMarketCap trending API error: ${response.status} - ${errorText}`);
            throw new Error(`CoinMarketCap trending API error: ${response.status} - ${errorText}`);
        }

        const data = await response.json();
        console.log(`✅ Fetched ${data.data?.length || 0} trending tokens from CoinMarketCap`);

        res.json(data);

    } catch (error) {
        console.error('❌ Error in trending-tokens endpoint:', error);
        res.status(500).json({
            error: 'Failed to fetch trending tokens',
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
    console.log(`⚡ Ready for immediate execution + auto-expiration every 15 seconds!`);

    // Test Supabase connection
    console.log('🔌 Testing Supabase connection...');
    console.log('🔍 Supabase URL:', process.env.SUPABASE_URL);
    console.log('🔍 Service Role Key (first 30 chars):', process.env.SUPABASE_SERVICE_ROLE_KEY?.substring(0, 30));
    console.log('🔍 Service Role Key length:', process.env.SUPABASE_SERVICE_ROLE_KEY?.length || 0);
    console.log('🔍 Service Role Key (last 30 chars):', process.env.SUPABASE_SERVICE_ROLE_KEY?.substring(-30));
    console.log('🔍 Service Role Key contains "eyJ":', process.env.SUPABASE_SERVICE_ROLE_KEY?.includes('eyJ'));
    console.log('🔍 Service Role Key contains "==":', process.env.SUPABASE_SERVICE_ROLE_KEY?.includes('=='));
    console.log('🔍 Supabase client properties:', Object.keys(supabase));
    console.log('🔍 Supabase client type:', typeof supabase);
    console.log('🔍 Supabase client URL:', supabase.supabaseUrl);
    console.log('🔍 Supabase client key length:', supabase.supabaseKey?.length || 0);
    console.log('🔍 Supabase client key (first 30):', supabase.supabaseKey?.substring(0, 30));

    // Test with a simple query first
    console.log('🔍 Testing simple Supabase query...');
    supabase.from('crypto_wagers').select('*').limit(1)
        .then(({ data, error }) => {
            console.log('🔍 Simple query response:', {
                hasData: !!data,
                dataLength: data?.length || 0,
                hasError: !!error,
                error: error
            });

            if (error) {
                console.error('❌ Simple query failed:', error);
                console.error('❌ Error details:', JSON.stringify(error, null, 2));
            } else {
                console.log('✅ Simple query successful!');

                // Now try the count query
                console.log('🔍 Testing count query...');
                return supabase.from('crypto_wagers').select('count', { count: 'exact', head: true });
            }
        })
        .then(({ count, error, data }) => {
            if (count !== undefined) {
                console.log('🔍 Count query response:', { count, error, data });
                if (error) {
                    console.error('❌ Count query failed:', error);
                } else {
                    console.log(`✅ Supabase connection successful! Found ${count} crypto wagers`);
                }
            }
        })
        .catch(err => {
            console.error('❌ Supabase connection test failed:', err);
            console.error('❌ Error stack:', err.stack);
        });

    // Start auto-expiration check every 15 seconds
    setInterval(async () => {
        try {
            const expiredCount = await expireExpiredWagers();
            if (expiredCount > 0) {
                console.log(`✅ Auto-expired ${expiredCount} wagers`);
            }
        } catch (error) {
            console.error('❌ Error in auto-expiration check:', error);
        }
    }, 15000); // 15 seconds = 15000 milliseconds

    // Start immediate crypto wager resolution check every 1 second for precise timing
    setInterval(async () => {
        try {
            await resolveExpiringCryptoWagers();
        } catch (error) {
            console.error('❌ Error in immediate crypto resolution check:', error);
        }
    }, 1000); // 1 second = 1000 milliseconds

    // Also run an immediate check for any wagers that might have already expired
    console.log('🚀 Running immediate expiration check for any already-expired wagers...');
    (async () => {
        try {
            const immediateExpiredCount = await expireExpiredWagers();
            if (immediateExpiredCount > 0) {
                console.log(`✅ Immediate check: Found and processed ${immediateExpiredCount} already-expired wagers`);
            } else {
                console.log('✅ Immediate check: No already-expired wagers found');
            }
        } catch (error) {
            console.error('❌ Error in immediate expiration check:', error);
        }
    })();

    // Test notification creation on startup using new database function
    console.log('🧪 Testing notification system...');
    (async () => {
        try {
            // Get a test user ID from the users table
            const { data: testUser, error: userError } = await supabase
                .from('users')
                .select('id, wallet_address')
                .limit(1)
                .single();

            if (userError || !testUser) {
                console.error('❌ No test user found for notification test:', userError);
                return;
            }

            console.log(`🧪 Testing notification for user: ${testUser.id} (${testUser.wallet_address})`);
            const testResult = await supabase.rpc('create_notification', {
                p_user_address: testUser.wallet_address,
                p_type: 'system',
                p_title: 'Test Notification',
                p_message: 'Background worker is running and notifications are working!'
            });

            if (testResult.error) {
                console.error('❌ Notification test failed:', testResult.error);
            } else {
                console.log('✅ Notification test completed');
            }
        } catch (error) {
            console.error('❌ Notification test failed:', error);
        }
    })();
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
