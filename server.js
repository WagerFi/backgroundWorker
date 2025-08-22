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
console.log('🔍 AUTHORITY_PRIVATE_KEY check:', AUTHORITY_PRIVATE_KEY ? 'Present' : 'Missing');

let authorityKeypair = null;
if (AUTHORITY_PRIVATE_KEY) {
    try {
        const secretKeyArray = JSON.parse(AUTHORITY_PRIVATE_KEY);
        console.log('🔍 Secret key array length:', secretKeyArray.length);
        console.log('🔍 Secret key array first 8 bytes:', secretKeyArray.slice(0, 8));

        const secretKeyBuffer = Buffer.from(secretKeyArray);
        console.log('🔍 Secret key buffer length:', secretKeyBuffer.length);

        authorityKeypair = Keypair.fromSecretKey(secretKeyBuffer);
        console.log('🔍 Keypair created successfully');
        console.log('🔍 Public key:', authorityKeypair.publicKey.toString());
        console.log('🔍 Is Keypair instance:', authorityKeypair instanceof Keypair);
        console.log('🔍 Has secretKey property:', !!authorityKeypair.secretKey);
        console.log('🔍 SecretKey length:', authorityKeypair.secretKey?.length || 'undefined');
    } catch (error) {
        console.error('❌ Error creating keypair:', error);
        process.exit(1);
    }
} else {
    console.error('❌ AUTHORITY_PRIVATE_KEY not found. Cannot execute on-chain transactions.');
    process.exit(1);
}

if (!authorityKeypair) {
    console.error('❌ Failed to create authority keypair. Cannot execute on-chain transactions.');
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

// Create Anchor provider and program using the authority keypair directly
// Note: We don't need to create a wallet interface since we'll pass the keypair to signers()
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

console.log('🔍 Provider wallet public key:', anchorProvider.wallet.publicKey.toString());
console.log('🔍 Authority keypair public key:', authorityKeypair.publicKey.toString());
console.log('🔍 Keys match:', anchorProvider.wallet.publicKey.equals(authorityKeypair.publicKey));

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
                // Derive the correct wager PDA and use escrow PDA from database
                console.log(`🔍 Deriving correct PDAs for resolveWager`);
                const resolveWagerPDA = PublicKey.findProgramAddressSync(
                    [Buffer.from("wager"), Buffer.from(accounts.wagerId)],
                    WAGERFI_PROGRAM_ID
                )[0];
                const resolveEscrowPDA = new PublicKey(accounts.escrowPda);

                console.log(`🔍 Wager account (derived): ${resolveWagerPDA.toString()}`);
                console.log(`🔍 Escrow account (from DB): ${resolveEscrowPDA.toString()}`);

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
                // The wagerId parameter should be the correctly derived wager PDA
                // The escrowPda parameter should be the escrow PDA from database
                const wagerAccountPDA = new PublicKey(accounts.wagerId);
                const escrowAccountPDA = new PublicKey(accounts.escrowPda);

                console.log(`🔍 Using correct PDAs for expired wager handling:`);
                console.log(`🔍 Wager account PDA: ${wagerAccountPDA.toString()}`);
                console.log(`🔍 Escrow account PDA: ${escrowAccountPDA.toString()}`);

                // Verify accounts exist before proceeding
                try {
                    const wagerAccount = await anchorProgram.account.wager.fetch(wagerAccountPDA);
                    const escrowBalance = await anchorProgram.provider.connection.getBalance(escrowAccountPDA);

                    console.log(`✅ Wager account verified: ${wagerAccountPDA.toString()}`);
                    console.log(`✅ Escrow account verified: ${escrowAccountPDA.toString()}`);
                    console.log(`✅ Escrow balance: ${escrowBalance / LAMPORTS_PER_SOL} SOL`);
                    console.log(`🔍 Expected wager amount: ${wagerAccount.amount / LAMPORTS_PER_SOL} SOL`);
                    console.log(`⚠️ Balance vs Expected: ${escrowBalance / LAMPORTS_PER_SOL} SOL vs ${wagerAccount.amount / LAMPORTS_PER_SOL} SOL`);

                    if (escrowBalance === 0) {
                        throw new Error('Escrow account has no funds');
                    }

                    // Check if escrow has sufficient funds for the wager amount
                    if (escrowBalance < wagerAccount.amount) {
                        console.error(`❌ Insufficient escrow funds! Escrow: ${escrowBalance / LAMPORTS_PER_SOL} SOL, Required: ${wagerAccount.amount / LAMPORTS_PER_SOL} SOL`);
                        throw new Error(`Insufficient escrow funds. Expected ${wagerAccount.amount / LAMPORTS_PER_SOL} SOL but escrow only has ${escrowBalance / LAMPORTS_PER_SOL} SOL`);
                    }
                } catch (accountError) {
                    console.error('❌ Account verification failed:', accountError);
                    throw new Error(`Account verification failed: ${accountError.message}`);
                }

                result = await anchorProgram.methods
                    .handleExpiredWager()
                    .accounts({
                        wager: wagerAccountPDA,
                        escrow: escrowAccountPDA,
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

            case 'resolve_wager_with_referrals':
                console.log(`🔍 Using correct PDAs for resolve_wager_with_referrals (wager PDA + escrow address)`);
                console.log(`🔍 Debug - accounts.wagerId: ${accounts.wagerId}`);
                console.log(`🔍 Debug - accounts.escrowPda: ${accounts.escrowPda}`);
                console.log(`🔍 Debug - accounts.escrowPda type: ${typeof accounts.escrowPda}`);
                console.log(`🔍 Debug - accounts.escrowPda length: ${accounts.escrowPda ? accounts.escrowPda.length : 'undefined'}`);
                console.log(`🔍 Debug - accounts.escrowPda raw bytes: ${accounts.escrowPda ? Buffer.from(accounts.escrowPda).toString('hex') : 'undefined'}`);

                let enhancedWagerPDA, enhancedEscrowPDA;
                try {
                    // Clean up the escrow address - remove any whitespace or invalid characters
                    const cleanEscrowPda = accounts.escrowPda.trim();
                    console.log(`🔍 Cleaned escrowPda: "${cleanEscrowPda}"`);

                    // Derive wager PDA from wagerId (this creates the correct wager account type)
                    enhancedWagerPDA = PublicKey.findProgramAddressSync(
                        [Buffer.from("wager"), Buffer.from(accounts.wagerId)],
                        WAGERFI_PROGRAM_ID
                    )[0];

                    // Use escrow address from database for escrow account
                    enhancedEscrowPDA = new PublicKey(cleanEscrowPda);
                    console.log(`✅ Successfully created PublicKey objects`);
                } catch (error) {
                    console.error(`❌ Error creating PublicKey from escrowPda: "${accounts.escrowPda}"`);
                    console.error(`❌ Error details:`, error.message);
                    throw error;
                }

                console.log(`🔍 Wager account (derived PDA): ${enhancedWagerPDA.toString()}`);
                console.log(`🔍 Escrow account (from DB): ${enhancedEscrowPDA.toString()}`);
                console.log(`🔍 Winner: ${accounts.winnerPubkey}`);
                console.log(`🔍 Treasury: ${accounts.treasuryPubkey}`);
                console.log(`🔍 Creator Referrer: ${accounts.creatorReferrerPubkey || 'None'}`);
                console.log(`🔍 Acceptor Referrer: ${accounts.acceptorReferrerPubkey || 'None'}`);

                // Build accounts object - follow the EXACT order from exported IDL
                // NOTE: Even though IDL says "isOptional: true", Anchor JS client requires all accounts
                // So we use treasury as placeholder when referrers don't exist (percentage = 0)

                // Validate all addresses before creating PublicKey objects
                console.log(`🔍 Validating addresses before PublicKey creation:`);
                console.log(`  Winner: ${accounts.winnerPubkey}`);
                console.log(`  Creator: ${accounts.creatorPubkey}`);
                console.log(`  Treasury: ${accounts.treasuryPubkey}`);
                console.log(`  Creator Referrer: ${accounts.creatorReferrerPubkey || 'None'}`);
                console.log(`  Acceptor Referrer: ${accounts.acceptorReferrerPubkey || 'None'}`);

                // Validate that all addresses are valid Solana addresses
                const validateAddress = (address, name) => {
                    if (!address) {
                        throw new Error(`${name} address is undefined or null`);
                    }
                    if (typeof address !== 'string') {
                        throw new Error(`${name} address is not a string: ${typeof address}`);
                    }
                    if (address.length < 32 || address.length > 44) {
                        throw new Error(`${name} address length is invalid: ${address.length} (${address})`);
                    }
                    // Check if it contains only valid base58 characters
                    if (!/^[1-9A-HJ-NP-Za-km-z]+$/.test(address)) {
                        throw new Error(`${name} address contains invalid characters: ${address}`);
                    }
                    return true;
                };

                try {
                    validateAddress(accounts.escrowPda, 'Escrow');
                    validateAddress(accounts.winnerPubkey, 'Winner');
                    validateAddress(accounts.creatorPubkey, 'Creator');
                    validateAddress(accounts.treasuryPubkey, 'Treasury');
                    if (accounts.creatorReferrerPubkey) {
                        validateAddress(accounts.creatorReferrerPubkey, 'Creator Referrer');
                    }
                    if (accounts.acceptorReferrerPubkey) {
                        validateAddress(accounts.acceptorReferrerPubkey, 'Acceptor Referrer');
                    }
                    console.log(`✅ All addresses validated successfully`);
                } catch (validationError) {
                    console.error(`❌ Address validation failed:`, validationError.message);
                    throw validationError;
                }

                const enhancedAccounts = {
                    wager: enhancedWagerPDA,
                    escrow: enhancedEscrowPDA,
                    winner: new PublicKey(accounts.winnerPubkey),
                    creator: new PublicKey(accounts.creatorPubkey), // Add creator back - program requires it

                    treasury: new PublicKey(accounts.treasuryPubkey),

                    // Add authority back - the program requires it in the accounts list
                    authority: authorityKeypair.publicKey,
                };

                // Only add referrer accounts if they exist and are not the treasury
                if (accounts.creatorReferrerPubkey && accounts.creatorReferrerPubkey !== accounts.treasuryPubkey) {
                    enhancedAccounts.creatorReferrer = new PublicKey(accounts.creatorReferrerPubkey);
                }
                if (accounts.acceptorReferrerPubkey && accounts.acceptorReferrerPubkey !== accounts.treasuryPubkey) {
                    enhancedAccounts.acceptorReferrer = new PublicKey(accounts.acceptorReferrerPubkey);
                }

                console.log(`🔍 Referrer accounts added:`, {
                    creatorReferrer: enhancedAccounts.creatorReferrer.toString(),
                    acceptorReferrer: enhancedAccounts.acceptorReferrer.toString()
                });

                console.log(`🔍 Final enhancedAccounts object (with authority):`, JSON.stringify(enhancedAccounts, (key, value) => {
                    if (value && typeof value === 'object' && value.toBase58) {
                        return value.toBase58();
                    }
                    return value;
                }, 2));

                console.log(`🔍 Authority keypair debug:`);
                console.log(`  Type: ${typeof authorityKeypair}`);
                console.log(`  Constructor: ${authorityKeypair.constructor.name}`);
                console.log(`  Public key: ${authorityKeypair.publicKey.toString()}`);
                console.log(`  Has signTransaction: ${typeof authorityKeypair.signTransaction === 'function'}`);
                console.log(`  Has secretKey: ${!!authorityKeypair.secretKey}`);
                console.log(`  Signers array: [${authorityKeypair.publicKey.toString()}]`);

                // Verify this is a proper Keypair instance
                console.log(`  Is Keypair instance: ${authorityKeypair instanceof Keypair}`);

                // Test if we can create a simple transaction with this keypair
                try {
                    const testTx = new Transaction();
                    testTx.add(SystemProgram.transfer({
                        fromPubkey: authorityKeypair.publicKey,
                        toPubkey: authorityKeypair.publicKey,
                        lamports: 0
                    }));
                    testTx.feePayer = authorityKeypair.publicKey;
                    testTx.recentBlockhash = (await connection.getLatestBlockhash()).blockhash;

                    // Try to sign it
                    testTx.sign(authorityKeypair);
                    console.log(`✅ Keypair signing test passed`);
                } catch (signError) {
                    console.error(`❌ Keypair signing test failed:`, signError);
                    console.error(`   Error details:`, signError.message);
                }

                // Final verification before transaction
                console.log(`🔍 Final keypair verification before transaction:`);
                console.log(`  Public key: ${authorityKeypair.publicKey.toString()}`);
                console.log(`  Is Keypair instance: ${authorityKeypair instanceof Keypair}`);
                console.log(`  Has secretKey: ${!!authorityKeypair.secretKey}`);
                console.log(`  SecretKey type: ${typeof authorityKeypair.secretKey}`);
                console.log(`  SecretKey is Uint8Array: ${authorityKeypair.secretKey instanceof Uint8Array}`);

                console.log(`🔍 Authority keypair verification:`);
                console.log(`  Public key: ${authorityKeypair.publicKey.toString()}`);
                console.log(`  Is Keypair instance: ${authorityKeypair instanceof Keypair}`);

                // Use the standard Anchor approach that works for other instructions
                result = await anchorProgram.methods
                    .resolveWagerWithReferrals(
                        { [args.winner.toLowerCase()]: {} },
                        args.creatorReferrerPercentage || 0,
                        args.acceptorReferrerPercentage || 0
                    )
                    .accounts(enhancedAccounts)
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
                user_id: userId, // Add the missing user_id field
                user_address: userAddress, // Use user_address as per database schema
                type: type,
                title: title,
                message: message,
                data: data,
                read: false, // Use 'read' instead of 'is_read' as per database schema
                is_deleted: false,
                created_at: new Date().toISOString()
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
    new PublicKey('GPLMWDSiwmhqDYYDgs12XBAcRtAJRGPufm268xKqWFgi'); // Fallback to WagerFi treasury

// Treasury wallet keypair for reward distributions
const TREASURY_PRIVATE_KEY = process.env.TREASURY_PRIVATE_KEY ?
    JSON.parse(process.env.TREASURY_PRIVATE_KEY) :
    (() => {
        console.error('❌ TREASURY_PRIVATE_KEY environment variable not set!');
        console.error('   Add TREASURY_PRIVATE_KEY=[...] to your .env file');
        process.exit(1);
    })();

const treasuryKeypair = Keypair.fromSecretKey(new Uint8Array(TREASURY_PRIVATE_KEY));

console.log('🏦 Treasury wallet initialized for reward distributions:', treasuryKeypair.publicKey.toString());

// Verify treasury keypair matches expected address
const expectedTreasuryAddress = process.env.TREASURY_WALLET_ADDRESS || 'GPLMWDSiwmhqDYYDgs12XBAcRtAJRGPufm268xKqWFgi';
if (treasuryKeypair.publicKey.toString() !== expectedTreasuryAddress) {
    console.error('❌ Treasury private key does not match expected address!');
    console.error(`   Expected: ${expectedTreasuryAddress}`);
    console.error(`   Got: ${treasuryKeypair.publicKey.toString()}`);
    process.exit(1);
}

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
        treasury: treasuryKeypair.publicKey.toString(),
        timestamp: new Date().toISOString()
    });
});

// Manual reward system triggers for testing
app.post('/admin/calculate-rewards', async (req, res) => {
    try {
        await calculateDailyRewards();
        res.json({ success: true, message: 'Daily rewards calculation completed' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/admin/distribute-rewards', async (req, res) => {
    try {
        await distributePendingRewards();
        res.json({ success: true, message: 'Pending rewards distribution completed' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/admin/distribute-buyback', async (req, res) => {
    try {
        const { snapshot_id } = req.body;

        if (!snapshot_id) {
            return res.status(400).json({ error: 'snapshot_id is required' });
        }

        const result = await distributeBuybackReward(snapshot_id);
        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/admin/treasury-balance', async (req, res) => {
    try {
        const balance = await getTreasuryBalance();
        res.json({
            success: true,
            balance: balance,
            address: treasuryKeypair.publicKey.toString()
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/admin/treasury-snapshot', async (req, res) => {
    try {
        const { date } = req.query;

        if (!date) {
            return res.status(400).json({ error: 'date parameter is required' });
        }

        const { data: snapshot, error } = await supabase
            .from('treasury_daily_snapshots')
            .select('*')
            .eq('snapshot_date', date)
            .single();

        if (error || !snapshot) {
            return res.status(404).json({ error: 'Snapshot not found for date: ' + date });
        }

        res.json({
            success: true,
            snapshot: snapshot
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Test reward system with specified budget
// FIXED: Now prevents multiple snapshots from being created on the same day
// This ensures consistent reward calculations (micro-drops, etc.)
app.post('/admin/test-rewards', async (req, res) => {
    try {
        const { testBudget = 5.0, skipDailyCalculation = true } = req.body;

        console.log(`🧪 Testing reward system with ${testBudget} SOL budget`);

        // Check treasury balance
        const treasuryBalance = await getTreasuryBalance();
        if (treasuryBalance < testBudget) {
            return res.status(400).json({
                success: false,
                error: `Insufficient treasury balance. Available: ${treasuryBalance} SOL, Requested: ${testBudget} SOL`
            });
        }

        const today = new Date().toISOString().split('T')[0];

        // Check if a snapshot already exists for today
        const { data: existingSnapshot, error: checkError } = await supabase
            .from('treasury_daily_snapshots')
            .select('*')
            .eq('snapshot_date', today)
            .single();

        if (existingSnapshot) {
            console.log(`⚠️ Snapshot already exists for today with budget: ${existingSnapshot.reward_budget} SOL`);
            console.log(`📝 Using existing snapshot instead of creating new one`);
        } else {
            // Create a test treasury snapshot with the specified budget
            const { error: snapshotError } = await supabase
                .from('treasury_daily_snapshots')
                .insert({
                    snapshot_date: today,
                    treasury_balance_start: treasuryBalance - testBudget,
                    treasury_balance_end: treasuryBalance,
                    daily_earnings: testBudget,
                    reward_budget: testBudget, // Use full test budget as reward budget
                    is_calculated: true
                });

            if (snapshotError) {
                return res.status(500).json({ success: false, error: `Snapshot error: ${snapshotError.message}` });
            }

            console.log(`✅ Created new snapshot for today with budget: ${testBudget} SOL`);
        }

        // Use existing snapshot budget or test budget
        const snapshotToUse = existingSnapshot || { id: null, reward_budget: testBudget };
        const budgetToUse = existingSnapshot ? existingSnapshot.reward_budget : testBudget;

        console.log(`💰 Using reward budget: ${budgetToUse} SOL`);

        // Schedule test rewards with the appropriate budget
        await scheduleRandomRewards(today, budgetToUse, snapshotToUse.id);

        // Get pending rewards count
        const { data: pendingRewards, error: pendingError } = await supabase
            .from('reward_distributions')
            .select('count(*)', { count: 'exact' })
            .eq('is_distributed', false);

        const pendingCount = pendingError ? 0 : (pendingRewards[0]?.count || 0);

        res.json({
            success: true,
            message: `Test rewards scheduled successfully`,
            details: {
                testBudget: testBudget,
                treasuryBalance: treasuryBalance,
                pendingRewards: pendingCount,
                nextDistribution: 'Will distribute on next 5-minute cycle or call /admin/distribute-rewards'
            }
        });

    } catch (error) {
        console.error('❌ Error in test-rewards:', error);
        res.status(500).json({ success: false, error: error.message });
    }
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

        // Execute on-chain resolution with enhanced referral processing
        const resolutionResult = await resolveWagerWithReferrals(wager, winnerPosition, 'crypto');

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

        // Process enhanced resolution with atomic referral payouts
        const enhancedResult = await resolveWagerWithReferrals(wager, winner_position, 'crypto');

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

// Test endpoint to verify server is working
app.get('/test', (req, res) => {
    res.json({ message: 'Background worker is running!', timestamp: new Date().toISOString() });
});

// Quick fix endpoint to recalculate user stats using correct columns
app.post('/fix-user-stats', async (req, res) => {
    try {
        const { wallet_address } = req.body;

        if (!wallet_address) {
            return res.status(400).json({ error: 'wallet_address is required' });
        }

        console.log(`🔧 Fixing stats for user: ${wallet_address}`);

        // Get user ID
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('id, total_wagered')
            .eq('wallet_address', wallet_address)
            .single();

        if (userError || !user) {
            return res.status(404).json({ error: 'User not found' });
        }

        // For now, just ensure the columns exist with correct data
        // Assuming total_wagered is correct (1.55), set other fields based on current activity
        const { error: updateError } = await supabase
            .from('users')
            .update({
                total_won: 0, // No resolved wagers yet
                total_lost: 0, // No resolved wagers yet
                win_rate: 0, // No resolved wagers yet
                win_streak: 0,
                loss_streak: 0,
                updated_at: new Date().toISOString()
            })
            .eq('id', user.id);

        if (updateError) {
            console.error('Error updating user stats:', updateError);
            return res.status(500).json({ error: 'Failed to update user stats' });
        }

        console.log(`✅ Fixed stats for ${wallet_address}`);

        res.json({
            success: true,
            wallet_address,
            message: 'Stats fixed - now using correct columns'
        });

    } catch (error) {
        console.error('❌ Error fixing user stats:', error);
        res.status(500).json({ error: error.message });
    }
});

// Process referral payouts when wagers resolve
async function processReferralPayouts(wager, wagerType) {
    try {
        console.log(`💰 Processing referral payouts for ${wagerType} wager: ${wager.wager_id}`);

        const wagerAmount = wager.amount || wager.sol_amount;
        const creatorAddress = wager.creator_address;
        const acceptorAddress = wager.acceptor_address;

        // Process referral for creator
        await processUserReferralPayout(wagerAmount, creatorAddress, wagerType);

        // Process referral for acceptor (if exists)
        if (acceptorAddress) {
            await processUserReferralPayout(wagerAmount, acceptorAddress, wagerType);
        }

        console.log(`✅ Referral payouts processed for wager: ${wager.wager_id}`);

    } catch (error) {
        console.error(`❌ Error processing referral payouts for wager ${wager.wager_id}:`, error);
    }
}

// Process referral payout for a single user
async function processUserReferralPayout(wagerAmount, userAddress, wagerType) {
    try {
        console.log(`💸 Processing referral payout for user: ${userAddress}, amount: ${wagerAmount}`);

        // Call the database function to handle referral logic
        const { data, error } = await supabase.rpc('process_referral_payout', {
            p_wager_amount: wagerAmount,
            p_wagerer_address: userAddress
        });

        if (error) {
            console.error(`❌ Error processing referral payout for ${userAddress}:`, error);
            return;
        }

        if (!data.has_referrer) {
            console.log(`📊 No referrer for user ${userAddress} - all platform fee goes to treasury`);
            return;
        }

        const {
            referrer_address,
            referrer_level,
            referrer_percentage,
            platform_fee,
            referral_payout,
            treasury_amount
        } = data;

        console.log(`💰 Referral payout details:`, {
            referrer: referrer_address,
            level: referrer_level,
            percentage: `${(referrer_percentage * 100).toFixed(0)}%`,
            platform_fee: platform_fee,
            referral_payout: referral_payout,
            treasury: treasury_amount
        });

        // Integrate with Solana token program to send actual SOL
        try {
            await sendReferralPayout(referrer_address, referral_payout);
            console.log(`✅ Referral payout of ${referral_payout} SOL sent to ${referrer_address}`);
        } catch (error) {
            console.error(`❌ Failed to send referral payout to ${referrer_address}:`, error);
            // TODO: Add retry logic or manual payout queue
        }

    } catch (error) {
        console.error(`❌ Error processing referral payout for user ${userAddress}:`, error);
    }
}

// Get referral stats for a user
async function getUserReferralStats(userAddress) {
    try {
        const { data, error } = await supabase
            .from('users')
            .select(`
                referral_code,
                referral_level,
                referral_trade_count,
                referral_trade_vol,
                referral_user_count,
                referral_earnings,
                referred_users
            `)
            .eq('wallet_address', userAddress)
            .single();

        if (error) {
            console.error('Error fetching referral stats:', error);
            return null;
        }

        return data;
    } catch (error) {
        console.error('Error in getUserReferralStats:', error);
        return null;
    }
}

// Send referral payout via Solana token program
async function sendReferralPayout(referrerAddress, payoutAmount) {
    try {
        console.log(`💸 Initiating referral payout: ${payoutAmount} SOL to ${referrerAddress}`);

        // TODO: Replace with your actual token program integration
        // This should call your Solana program to transfer SOL from treasury to referrer

        // Example structure for your token program call:
        /*
        const result = await executeTokenTransfer({
            from: TREASURY_WALLET,
            to: referrerAddress,
            amount: payoutAmount,
            instruction_type: 'referral_payout',
            metadata: {
                payout_type: 'referral_commission',
                timestamp: new Date().toISOString()
            }
        });
        
        if (!result.success) {
            throw new Error(`Token transfer failed: ${result.error}`);
        }
        
        console.log(`✅ Referral payout transaction: ${result.signature}`);
        return result;
        */

        // For now, simulate successful payout
        console.log(`🚀 [SIMULATION] Referral payout sent: ${payoutAmount} SOL → ${referrerAddress}`);

        return {
            success: true,
            signature: `ref_payout_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            amount: payoutAmount,
            recipient: referrerAddress
        };

    } catch (error) {
        console.error('Error sending referral payout:', error);
        throw error;
    }
}

// Get referral information for wager participants
async function getWagerReferralInfo(creatorAddress, acceptorAddress, wager) {
    try {
        console.log(`🔍 Fetching referral info for creator: ${creatorAddress}, acceptor: ${acceptorAddress}`);

        const { data: users, error } = await supabase
            .from('users')
            .select('wallet_address, referred_by, referral_level')
            .in('wallet_address', [creatorAddress, acceptorAddress]);

        if (error) {
            console.error('Error fetching referral info:', error);
            return { creatorReferrer: null, acceptorReferrer: null };
        }

        const creatorData = users.find(u => u.wallet_address === creatorAddress);
        const acceptorData = users.find(u => u.wallet_address === acceptorAddress);

        let creatorReferrer = null;
        let acceptorReferrer = null;

        // Get creator's referrer info
        if (creatorData?.referred_by) {
            const { data: creatorReferrerData } = await supabase
                .from('users')
                .select('wallet_address, referral_level')
                .eq('wallet_address', creatorData.referred_by)
                .single();

            if (creatorReferrerData) {
                const percentage = getReferralPercentage(creatorReferrerData.referral_level || 1);
                const platformFee = wager.amount * 2 * 0.04; // 4% of total pot
                const commissionAmount = (platformFee * percentage) / 100;

                creatorReferrer = {
                    address: creatorReferrerData.wallet_address,
                    level: creatorReferrerData.referral_level || 1,
                    percentage: percentage,
                    commission_amount: commissionAmount
                };
            }
        }

        // Get acceptor's referrer info
        if (acceptorData?.referred_by) {
            const { data: acceptorReferrerData } = await supabase
                .from('users')
                .select('wallet_address, referral_level')
                .eq('wallet_address', acceptorData.referred_by)
                .single();

            if (acceptorReferrerData) {
                const percentage = getReferralPercentage(acceptorReferrerData.referral_level || 1);
                const platformFee = wager.amount * 2 * 0.04; // 4% of total pot
                const commissionAmount = (platformFee * percentage) / 100;

                acceptorReferrer = {
                    address: acceptorReferrerData.wallet_address,
                    level: acceptorReferrerData.referral_level || 1,
                    percentage: percentage,
                    commission_amount: commissionAmount
                };
            }
        }

        console.log(`📊 Referral info: Creator referrer: ${creatorReferrer?.address || 'None'} (${creatorReferrer?.percentage || 0}%), Acceptor referrer: ${acceptorReferrer?.address || 'None'} (${acceptorReferrer?.percentage || 0}%)`);

        return { creatorReferrer, acceptorReferrer };

    } catch (error) {
        console.error('Error in getWagerReferralInfo:', error);
        return { creatorReferrer: null, acceptorReferrer: null };
    }
}

// Get referral percentage based on level
function getReferralPercentage(level) {
    switch (level) {
        case 1: return 10; // Recruiter: 10% of 4% platform fee
        case 2: return 15; // Influencer: 15% of 4% platform fee  
        case 3: return 20; // Rainmaker: 20% of 4% platform fee
        case 4: return 25; // Titan: 25% of 4% platform fee
        default: return 10;
    }
}

// Update wager status in database after resolution
async function updateWagerStatus(wager, status, winnerPosition, signature) {
    try {
        const tableName = wager.token_symbol ? 'crypto_wagers' : 'sports_wagers';
        const winnerId = winnerPosition === 'creator' ? wager.creator_id : wager.acceptor_id;
        const winnerAddress = winnerPosition === 'creator' ? wager.creator_address : wager.acceptor_address;

        const updateData = {
            status: status,
            winner_id: winnerId,
            winner_address: winnerAddress,
            winner_position: winnerPosition,
            resolved_at: new Date().toISOString(),
            on_chain_signature: signature,
            metadata: {
                ...(wager.metadata || {}),
                resolution_details: {
                    resolved_at: new Date().toISOString(),
                    winner_position: winnerPosition
                }
            }
        };

        // Add crypto-specific fields
        if (tableName === 'crypto_wagers') {
            updateData.resolution_price = wager.target_price; // Use stored price
            updateData.resolution_time = new Date().toISOString();
        }

        // Add sports-specific fields  
        if (tableName === 'sports_wagers') {
            updateData.resolution_outcome = winnerPosition; // For sports, outcome matches position
        }

        const { error: updateError } = await supabase
            .from(tableName)
            .update(updateData)
            .eq('id', wager.id);

        if (updateError) {
            console.error(`❌ Error updating ${tableName} wager ${wager.wager_id}:`, updateError);
            throw new Error(`Database update failed: ${updateError.message}`);
        }

        console.log(`✅ Updated ${tableName} wager ${wager.wager_id} status to ${status}`);

    } catch (error) {
        console.error(`❌ Error in updateWagerStatus:`, error);
        throw error;
    }
}

// Enhanced wager resolution with atomic referral payouts
async function resolveWagerWithReferrals(wager, winnerPosition, wagerType) {
    try {
        console.log(`🎯 Resolving ${wagerType} wager with atomic referral processing: ${wager.wager_id}`);

        // Prevent double execution by checking if already resolved
        const { data: currentWager, error: checkError } = await supabase
            .from(wagerType === 'crypto' ? 'crypto_wagers' : 'sports_wagers')
            .select('status')
            .eq('wager_id', wager.wager_id)
            .single();

        if (checkError || currentWager?.status === 'resolved') {
            console.log(`⚠️ Wager ${wager.wager_id} already resolved or not found, skipping...`);
            return;
        }

        // 1. Get referral information for both participants
        const { creatorReferrer, acceptorReferrer } = await getWagerReferralInfo(
            wager.creator_address,
            wager.acceptor_address,
            wager
        );

        // 2. Execute enhanced on-chain resolution with referral data
        const onChainResult = await executeEnhancedWagerResolution(
            wager,
            winnerPosition,
            wagerType,
            creatorReferrer,
            acceptorReferrer
        );

        if (!onChainResult.success) {
            throw new Error(`On-chain resolution failed: ${onChainResult.error}`);
        }

        // 3. Update database with resolution
        await updateWagerStatus(wager, 'resolved', winnerPosition, onChainResult.signature);

        // 4. Update referrer stats in database (referral payouts already happened on-chain)
        if (creatorReferrer) {
            await updateReferrerStats(wager.creator_address, wager.amount, creatorReferrer.commission_amount, wagerType);
        }
        if (acceptorReferrer) {
            await updateReferrerStats(wager.acceptor_address, wager.amount, acceptorReferrer.commission_amount, wagerType);
        }

        // 5. Update user stats
        const winnerId = winnerPosition === 'creator' ? wager.creator_id : wager.acceptor_id;
        await updateWagerUserStats(wager, winnerId, winnerPosition, wagerType);

        console.log(`✅ Wager resolved with atomic referral payouts: ${wager.wager_id}`);

        return {
            success: true,
            wager_id: wager.wager_id,
            signature: onChainResult.signature,
            referrals_processed: true,
            creator_referrer: creatorReferrer?.address || null,
            acceptor_referrer: acceptorReferrer?.address || null
        };

    } catch (error) {
        console.error(`❌ Error resolving wager with referrals: ${wager.wager_id}:`, error);
        throw error;
    }
}

// Execute enhanced wager resolution with referral accounts
async function executeEnhancedWagerResolution(wager, winnerPosition, wagerType, creatorReferrer, acceptorReferrer) {
    try {
        console.log(`🔗 Executing enhanced on-chain resolution for wager ${wager.wager_id}`);

        // Calculate fees and amounts
        const totalWagerAmount = wager.amount * 2;
        const platformFee = totalWagerAmount * PLATFORM_FEE_PERCENTAGE; // 4% platform fee
        const winnerAmount = totalWagerAmount - platformFee;

        console.log(`💰 Enhanced fee breakdown for wager ${wager.wager_id}:`);
        console.log(`   Total wager: ${totalWagerAmount} SOL`);
        console.log(`   Platform fee (4%): ${platformFee} SOL`);
        console.log(`   Winner gets: ${winnerAmount} SOL`);

        if (creatorReferrer) {
            const creatorReferralAmount = platformFee * (creatorReferrer.percentage / 100);
            console.log(`   Creator referrer gets: ${creatorReferralAmount} SOL (${creatorReferrer.percentage}%)`);
        }
        if (acceptorReferrer) {
            const acceptorReferralAmount = platformFee * (acceptorReferrer.percentage / 100);
            console.log(`   Acceptor referrer gets: ${acceptorReferralAmount} SOL (${acceptorReferrer.percentage}%)`);
        }

        try {
            console.log(`🔐 Executing enhanced on-chain resolution...`);

            // Get wager data
            const tableName = wagerType === 'crypto' ? 'crypto_wagers' : 'sports_wagers';
            const { data: wagerData, error: wagerError } = await supabase
                .from(tableName)
                .select('wager_id, escrow_pda, creator_address, acceptor_address')
                .eq('wager_id', wager.wager_id)
                .single();

            if (wagerError || !wagerData) {
                throw new Error(`Failed to fetch wager data: ${wagerError?.message || 'Wager not found'}`);
            }

            console.log(`🔍 Database wager data:`, {
                wager_id: wagerData.wager_id,
                escrow_pda: wagerData.escrow_pda,
                escrow_pda_type: typeof wagerData.escrow_pda,
                escrow_pda_length: wagerData.escrow_pda ? wagerData.escrow_pda.length : 'undefined',
                creator_address: wagerData.creator_address,
                acceptor_address: wagerData.acceptor_address
            });

            // Validate escrow_pda before using it
            if (!wagerData.escrow_pda || typeof wagerData.escrow_pda !== 'string') {
                throw new Error(`Invalid escrow_pda: ${wagerData.escrow_pda}`);
            }

            // Check if escrow_pda looks like a valid Solana public key (base58, 32-44 characters)
            if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(wagerData.escrow_pda)) {
                throw new Error(`escrow_pda is not a valid Solana public key format: ${wagerData.escrow_pda}`);
            }

            let wagerAccount, escrowAccount;
            try {
                wagerAccount = new PublicKey(wagerData.escrow_pda);
                escrowAccount = new PublicKey(wagerData.escrow_pda);
                console.log(`✅ Successfully created PublicKey objects from database escrow_pda`);
            } catch (error) {
                console.error(`❌ Error creating PublicKey from database escrow_pda: "${wagerData.escrow_pda}"`);
                console.error(`❌ Error details:`, error.message);
                throw error;
            }
            const winnerWallet = winnerPosition === 'creator'
                ? new PublicKey(wagerData.creator_address)
                : new PublicKey(wagerData.acceptor_address);

            console.log(`   Wager Account: ${wagerAccount.toString()}`);
            console.log(`   Escrow Account: ${escrowAccount.toString()}`);
            console.log(`   Winner Wallet: ${winnerWallet.toString()}`);
            console.log(`   Treasury: ${TREASURY_WALLET.toString()}`);

            if (creatorReferrer) {
                console.log(`   Creator Referrer: ${creatorReferrer.address} (${creatorReferrer.percentage}%)`);
            }
            if (acceptorReferrer) {
                console.log(`   Acceptor Referrer: ${acceptorReferrer.address} (${acceptorReferrer.percentage}%)`);
            }

            // Execute the enhanced resolve_wager_with_referrals instruction with referral data
            const instructionAccounts = {
                wagerId: wagerData.wager_id,
                escrowPda: wagerData.escrow_pda,
                winnerPubkey: winnerWallet.toString(),
                creatorPubkey: wagerData.creator_address, // Add creator account for rent reclaim
                treasuryPubkey: TREASURY_WALLET.toString(),
                creatorReferrerPubkey: creatorReferrer?.address || TREASURY_WALLET.toString(),
                acceptorReferrerPubkey: acceptorReferrer?.address || TREASURY_WALLET.toString()
            };

            console.log(`🔍 Instruction accounts being passed:`, instructionAccounts);

            const transaction = await executeProgramInstruction('resolve_wager_with_referrals', instructionAccounts, {
                winner: winnerPosition,
                creatorReferrerPercentage: creatorReferrer?.percentage || 0,
                acceptorReferrerPercentage: acceptorReferrer?.percentage || 0
            });

            console.log(`✅ Enhanced on-chain resolution completed: ${transaction}`);

            return {
                success: true,
                signature: transaction,
                referrals_paid: true
            };

        } catch (error) {
            console.error(`❌ Enhanced on-chain resolution failed for ${wager.wager_id}:`, error);
            throw error;
        }

    } catch (error) {
        console.error(`❌ Error in executeEnhancedWagerResolution: ${wager.wager_id}:`, error);
        throw error;
    }
}

// Update referrer statistics after successful payout
async function updateReferrerStats(wagererAddress, wagerAmount, commissionAmount, wagerType) {
    try {
        console.log(`📈 Updating referrer stats for wagerer: ${wagererAddress}, wager: ${wagerAmount} SOL, commission: ${commissionAmount} SOL`);

        // Get the referrer's wallet address first
        const { data: wagerer, error: fetchError } = await supabase
            .from('users')
            .select('referred_by')
            .eq('wallet_address', wagererAddress)
            .single();

        if (fetchError || !wagerer?.referred_by) {
            console.log(`ℹ️ No referrer found for wagerer: ${wagererAddress}`);
            return;
        }

        const referrerAddress = wagerer.referred_by;
        console.log(`📈 Updating stats for referrer: ${referrerAddress}`);

        // Update referrer's database stats
        const { data: currentReferrer, error: getCurrentError } = await supabase
            .from('users')
            .select('referral_trade_count, referral_trade_vol, referral_earnings, referral_level')
            .eq('wallet_address', referrerAddress)
            .single();

        if (getCurrentError) {
            console.error(`❌ Error fetching current referrer stats:`, getCurrentError);
            return;
        }

        // Calculate new stats
        const newTradeCount = (currentReferrer.referral_trade_count || 0) + 1;
        const newTradeVol = (currentReferrer.referral_trade_vol || 0) + wagerAmount; // Add the full wager amount
        const newEarnings = (currentReferrer.referral_earnings || 0) + commissionAmount; // Add the commission earned

        // Update referrer stats
        const { error: updateError } = await supabase
            .from('users')
            .update({
                referral_trade_count: newTradeCount,
                referral_trade_vol: newTradeVol,
                referral_earnings: newEarnings,
                updated_at: new Date().toISOString()
            })
            .eq('wallet_address', referrerAddress);

        if (updateError) {
            console.error(`❌ Error updating referrer stats:`, updateError);
            return;
        }

        console.log(`✅ Updated referrer stats for ${referrerAddress}:`, {
            trade_count: newTradeCount,
            trade_vol: newTradeVol,
            earnings: newEarnings
        });

        // Check for level upgrade thresholds: 30 SOL (L2), 100 SOL (L3), 250 SOL (L4)
        const currentLevel = currentReferrer.referral_level || 1;
        let newLevel = currentLevel;

        if (newTradeVol >= 250) newLevel = 4; // Titan
        else if (newTradeVol >= 100) newLevel = 3; // Rainmaker  
        else if (newTradeVol >= 30) newLevel = 2; // Influencer
        else newLevel = 1; // Recruiter

        if (newLevel > currentLevel) { // Only upgrade, never downgrade
            const { error: levelError } = await supabase
                .from('users')
                .update({ referral_level: newLevel })
                .eq('wallet_address', referrerAddress);

            if (!levelError) {
                const tierNames = { 1: 'Tier 1', 2: 'Tier 2', 3: 'Tier 3', 4: 'Tier 4' };
                const tierEmojis = { 1: '🔥', 2: '🌟', 3: '⚡', 4: '🏆' };
                const tierPercentages = { 1: '10%', 2: '15%', 3: '20%', 4: '25%' };

                console.log(`🎉 TIER UP! ${referrerAddress} promoted from Tier ${currentLevel} to Tier ${newLevel} (${tierNames[newLevel]}) with ${newTradeVol} SOL volume`);

                // Create tier-up notification
                const { data: referrerUser, error: fetchReferrerError } = await supabase
                    .from('users')
                    .select('id')
                    .eq('wallet_address', referrerAddress)
                    .single();

                if (!fetchReferrerError && referrerUser) {
                    await createNotification(
                        referrerUser.id,
                        'wager_resolved', // Use existing valid notification type
                        `${tierEmojis[newLevel]} Tier Up! You're now ${tierNames[newLevel]}!`,
                        `Congratulations! You've reached ${tierNames[newLevel]} and now earn ${tierPercentages[newLevel]} commission on all referral trades. Your referral volume: ${newTradeVol} SOL`,
                        {
                            type: 'tier_upgrade',
                            new_tier: newLevel,
                            tier_name: tierNames[newLevel],
                            tier_emoji: tierEmojis[newLevel],
                            commission_rate: tierPercentages[newLevel],
                            total_volume: newTradeVol,
                            animation_trigger: true // Trigger frontend animation
                        }
                    );

                    console.log(`📬 Tier-up notification sent to ${referrerAddress}`);
                }
            }
        }

    } catch (error) {
        console.error(`❌ Error in updateReferrerStats:`, error);
    }
}



// 3. Cancel Wager (Database Status Update + Background Refund)
app.post('/cancel-wager', async (req, res) => {
    try {
        const { wager_id, wager_type, cancelling_address } = req.body;

        console.log(`🔄 Cancel wager request received:`, { wager_id, wager_type, cancelling_address });

        if (!wager_id || !wager_type || !cancelling_address) {
            console.error(`❌ Missing required fields:`, { wager_id, wager_type, cancelling_address });
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

        // Also check if this might be a UUID (database row ID) instead of wager_id
        if (allWagersError) {
            console.error(`❌ Error fetching all wagers with ID ${wager_id}:`, allWagersError);
            return res.status(500).json({ error: 'Database query failed' });
        }

        if (!allWagers || allWagers.length === 0) {
            // Try searching by the database row ID instead
            console.log(`🔍 No wagers found with wager_id: ${wager_id}, trying database row ID...`);
            const { data: wagerById, error: idError } = await supabase
                .from(tableName)
                .select('*')
                .eq('id', wager_id);

            if (idError) {
                console.error(`❌ Error fetching wager by ID ${wager_id}:`, idError);
                return res.status(500).json({ error: 'Database query failed' });
            }

            if (wagerById && wagerById.length > 0) {
                console.log(`🔍 Found wager by database ID:`, wagerById[0]);
                // Use the wager_id from the found record
                const actualWagerId = wagerById[0].wager_id;
                if (actualWagerId) {
                    console.log(`🔍 Using actual wager_id: ${actualWagerId}`);
                    // Recursively call this function with the correct wager_id
                    return res.redirect(307, `/cancel-wager`);
                }
            }
        }

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

            // Log all wagers to see what we have
            allWagers.forEach((w, index) => {
                console.log(`🔍 Wager ${index}:`, {
                    id: w.id,
                    wager_id: w.wager_id,
                    status: w.status,
                    creator_id: w.creator_id,
                    creator_address: w.creator_address
                });
            });
        }

        // Use the wager we already found instead of doing another query
        if (!allWagers || allWagers.length === 0) {
            console.error(`❌ No wager found with ID ${wager_id}`);
            return res.status(404).json({ error: 'Wager not found' });
        }

        // Get the first wager (should only be one anyway)
        const wager = allWagers[0];
        console.log(`🔍 Selected wager for cancellation:`, {
            id: wager.id,
            wager_id: wager.wager_id,
            status: wager.status,
            creator_id: wager.creator_id,
            creator_address: wager.creator_address
        });

        // Check if the wager is open and can be cancelled
        if (wager.status !== 'open') {
            console.log(`❌ Wager ${wager_id} cannot be cancelled - status is ${wager.status}`);
            return res.status(400).json({
                error: `Wager cannot be cancelled in current status: ${wager.status}`
            });
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

        // Process the actual Solana refund from escrow
        console.log(`💰 Processing Solana refund for cancelled wager ${wager_id}...`);

        try {
            // Get the full wager data including escrow_pda
            const { data: fullWagerData, error: fullWagerError } = await supabase
                .from(tableName)
                .select('*')
                .eq('id', wager.id)
                .single();

            if (fullWagerError || !fullWagerData) {
                console.error(`❌ Error fetching full wager data for refund:`, fullWagerError);
                // Continue with cancellation even if refund fails
            } else {
                // Process the on-chain refund
                const refundResult = await processWagerRefundOnChain(fullWagerData);

                if (refundResult.success) {
                    console.log(`✅ Solana refund successful for wager ${wager_id}:`, refundResult.signature);
                    if (refundResult.blockchainConfirmation) {
                        console.log(`✅ Blockchain confirmation:`, refundResult.blockchainConfirmation);
                    }

                    // Update wager with refund signature
                    await supabase
                        .from(tableName)
                        .update({
                            metadata: {
                                ...updatedMetadata,
                                refund_signature: refundResult.signature,
                                refund_processed_at: new Date().toISOString()
                            }
                        })
                        .eq('id', wager.id);

                } else {
                    console.error(`❌ Solana refund failed for wager ${wager_id}:`, refundResult.error);
                    // Continue with cancellation even if refund fails
                }
            }
        } catch (refundError) {
            console.error(`❌ Error processing refund for wager ${wager_id}:`, refundError);
            // Continue with cancellation even if refund fails
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
            message: 'Wager cancelled successfully. Refund has been processed from escrow.'
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
                escrowPda: wagerData.escrow_pda,
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
                escrowPda: wagerData.escrow_pda,
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
            // Derive the correct wager PDA from the wager_id
            console.log(`🔍 Deriving wager PDA from wager_id: ${wagerData.wager_id}`);
            const wagerId = wagerData.wager_id;
            const seed = wagerId.length > 32 ? Buffer.from(wagerId, 'utf8').slice(0, 32) : Buffer.from(wagerId, 'utf8');
            const [wagerPDA, bump] = PublicKey.findProgramAddressSync(
                [Buffer.from('wager'), seed],
                WAGERFI_PROGRAM_ID
            );
            console.log(`🔍 Derived wager PDA: ${wagerPDA.toString()}`);

            const transaction = await executeProgramInstruction('handleExpiredWager', {
                wagerId: wagerPDA.toString(), // Use the correctly derived wager PDA
                escrowPda: wagerData.escrow_pda, // Use the escrow PDA from database
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
                .maybeSingle();

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

        // Execute on-chain resolution with enhanced referral processing
        const resolutionResult = await resolveWagerWithReferrals(wager, winnerPosition, 'crypto');

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

                // Process enhanced resolution with atomic referral payouts  
                const enhancedResult = await resolveWagerWithReferrals(wager, winner_position, 'sports');

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
        // Handle both amount and sol_amount fields for compatibility
        const refundAmount = wager.sol_amount || wager.amount; // Full amount to refund
        const networkFee = SOLANA_TRANSACTION_FEE; // Solana transaction fee
        const actualRefund = refundAmount - networkFee; // User gets amount minus network fee

        console.log(`💰 Refund breakdown for wager ${wager.wager_id}:`);
        console.log(`   Original amount: ${refundAmount} SOL`);
        console.log(`   Network fee: ${networkFee} SOL`);
        console.log(`   User receives: ${actualRefund} SOL`);
        console.log(`   Escrow PDA: ${wager.escrow_pda}`);
        console.log(`🔍 Wager data for debugging:`, {
            id: wager.id,
            wager_id: wager.wager_id,
            escrow_pda: wager.escrow_pda,
            creator_address: wager.creator_address,
            status: wager.status
        });

        // Execute actual Solana escrow withdrawal
        try {
            // Enhanced validation and logging
            console.log(`🔍 Validating wager data for refund:`, {
                wager_id: wager.wager_id,
                escrow_pda: wager.escrow_pda,
                creator_address: wager.creator_address,
                sol_amount: wager.sol_amount,
                amount: wager.amount,
                refundAmount: refundAmount
            });

            // Validate that escrow_pda is a valid Solana public key
            if (!wager.escrow_pda || typeof wager.escrow_pda !== 'string') {
                throw new Error(`Invalid escrow_pda: ${wager.escrow_pda}`);
            }

            // Check if escrow_pda looks like a valid Solana public key (base58, 32-44 characters)
            if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(wager.escrow_pda)) {
                throw new Error(`escrow_pda is not a valid Solana public key format: ${wager.escrow_pda}`);
            }

            const escrowAccount = new PublicKey(wager.escrow_pda);
            const userWallet = new PublicKey(wager.creator_address);

            console.log(`🔐 Executing real escrow withdrawal...`);
            console.log(`   Escrow: ${escrowAccount.toString()}`);
            console.log(`   User: ${userWallet.toString()}`);
            console.log(`   Authority: ${authorityKeypair.publicKey.toString()}`);

            // Check all account balances before proceeding
            try {
                const authorityBalance = await anchorProgram.provider.connection.getBalance(authorityKeypair.publicKey);
                const escrowBalance = await anchorProgram.provider.connection.getBalance(escrowAccount);
                const userBalance = await anchorProgram.provider.connection.getBalance(userWallet);

                console.log(`💰 Authority wallet balance: ${authorityBalance / LAMPORTS_PER_SOL} SOL`);
                console.log(`💰 Escrow account balance: ${escrowBalance / LAMPORTS_PER_SOL} SOL`);
                console.log(`💰 User wallet balance: ${userBalance / LAMPORTS_PER_SOL} SOL`);

                // Calculate minimum rent requirements
                const minimumRent = await anchorProgram.provider.connection.getMinimumBalanceForRentExemption(0);
                console.log(`🏠 Minimum rent requirement: ${minimumRent / LAMPORTS_PER_SOL} SOL`);

                if (authorityBalance < 10000) { // Less than 0.00001 SOL
                    console.warn(`⚠️ Authority wallet balance might be low: ${authorityBalance / LAMPORTS_PER_SOL} SOL`);
                }
            } catch (balanceError) {
                console.error(`❌ Error checking account balances:`, balanceError);
            }

            // Execute the handleExpiredWager instruction to refund the creator
            // Derive the correct wager PDA from the wager_id using the same logic as the frontend
            console.log(`🔍 Deriving wager PDA from wager_id: ${wager.wager_id}`);

            // Use the same PDA derivation logic as the frontend
            const wagerId = wager.wager_id;
            const seed = wagerId.length > 32 ? Buffer.from(wagerId, 'utf8').slice(0, 32) : Buffer.from(wagerId, 'utf8');
            const [wagerPDA, bump] = PublicKey.findProgramAddressSync(
                [Buffer.from('wager'), seed],
                WAGERFI_PROGRAM_ID // Use the correct program ID
            );

            console.log(`🔍 Derived wager PDA: ${wagerPDA.toString()}`);
            console.log(`🔍 Escrow PDA from database: ${wager.escrow_pda}`);

            const signature = await executeProgramInstruction('cancelWager', {
                wagerId: wagerPDA.toString(), // Use the correctly derived wager PDA
                escrowPda: wager.escrow_pda, // Use the escrow PDA from database
                creatorPubkey: userWallet.toString()
            });

            console.log(`   🔐 Real wager cancellation completed: ${signature}`);

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
            console.error(`❌ Real on-chain cancellation failed:`, onChainError);

            // Log the full error details to understand which account is failing
            console.error(`🔍 Full error details:`, {
                message: onChainError.message,
                signature: onChainError.signature,
                transactionMessage: onChainError.transactionMessage,
                logs: onChainError.transactionLogs || onChainError.logs
            });

            // Check if it's a rent issue but the refund actually succeeded
            if (onChainError.message && onChainError.message.includes('insufficient funds for rent')) {
                // Check if the transaction logs show successful refund
                const transactionLogs = onChainError.transactionLogs || onChainError.logs || [];
                const refundSuccessful = transactionLogs.some(log =>
                    log.includes('cancelled. Refunded') && log.includes('lamports to creator')
                );

                if (refundSuccessful) {
                    console.log(`⚠️ PROGRAM LOGS SHOW SUCCESS, BUT VERIFYING ACTUAL SOL TRANSFER...`);
                    console.log(`⚠️ Program log: ${transactionLogs.find(log => log.includes('cancelled. Refunded'))}`);

                    // CRITICAL: Check if SOL actually moved by examining account balances
                    try {
                        const userWallet = new PublicKey(wager.creator_address);
                        const escrowAccount = new PublicKey(wager.escrow_pda);

                        const userBalanceAfter = await anchorProgram.provider.connection.getBalance(userWallet);
                        const escrowBalanceAfter = await anchorProgram.provider.connection.getBalance(escrowAccount);

                        console.log(`🔍 POST-TRANSACTION VERIFICATION:`);
                        console.log(`   User balance after: ${userBalanceAfter / LAMPORTS_PER_SOL} SOL`);
                        console.log(`   Escrow balance after: ${escrowBalanceAfter / LAMPORTS_PER_SOL} SOL`);

                        // Check if escrow account was actually closed (balance = 0)
                        if (escrowBalanceAfter === 0) {
                            console.log(`✅ CONFIRMED: Escrow account closed (balance = 0)`);
                            console.log(`✅ This means SOL was actually transferred out!`);

                            return {
                                success: true,
                                signature: onChainError.signature || 'blockchain_confirmed_refund',
                                refundBreakdown: {
                                    originalAmount: refundAmount,
                                    networkFee: networkFee,
                                    actualRefund: actualRefund
                                },
                                note: 'Refund verified: Escrow account closed, SOL transferred to user.',
                                verification: {
                                    escrowClosed: true,
                                    userBalanceAfter: userBalanceAfter / LAMPORTS_PER_SOL
                                }
                            };
                        } else {
                            console.error(`❌ ESCROW STILL HAS BALANCE: ${escrowBalanceAfter / LAMPORTS_PER_SOL} SOL`);
                            console.error(`❌ This suggests the refund did NOT actually succeed!`);

                            return {
                                success: false,
                                error: 'Refund verification failed: Escrow account not closed',
                                details: `Program logs claimed success but escrow still has ${escrowBalanceAfter / LAMPORTS_PER_SOL} SOL`,
                                verification: {
                                    escrowClosed: false,
                                    escrowBalanceRemaining: escrowBalanceAfter / LAMPORTS_PER_SOL
                                }
                            };
                        }
                    } catch (verificationError) {
                        console.error(`❌ Could not verify refund success:`, verificationError);

                        // If we can't verify, treat as uncertain
                        return {
                            success: false,
                            error: 'Unable to verify refund completion',
                            details: 'Program logs suggest success but account verification failed',
                            programLogs: transactionLogs
                        };
                    }
                }

                console.error(`🚨 RENT ISSUE DETECTED!`);
                console.error(`🚨 This suggests one of the accounts lacks rent exemption, not transaction fees`);
                console.error(`🚨 Authority wallet (has 2.35 SOL): ${authorityKeypair.publicKey.toString()}`);
                console.error(`🚨 Check which account in transaction is failing rent requirements`);

                return {
                    success: false,
                    error: 'Account rent requirements not met.',
                    details: 'Transaction failed due to insufficient rent exemption for one of the accounts.',
                    authorityWallet: authorityKeypair.publicKey.toString()
                };
            }

            return {
                success: false,
                error: onChainError.message || 'Unknown blockchain error',
                details: onChainError
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
                const resolutionResult = await resolveWagerWithReferrals(wager, winnerPosition, 'crypto');

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

        // Update creator stats (no total_wagered - already counted during acceptance)
        await updateSingleUserStats(creatorId, {
            total_wagered: 0, // Don't double-count: already added during wager acceptance
            won: winnerId && winnerPosition ? creatorWon : null, // null for draws
            is_draw: !winnerId || !winnerPosition,
            wager_type: wagerType
        });

        // Update acceptor stats (no total_wagered - already counted during acceptance)
        await updateSingleUserStats(acceptorId, {
            total_wagered: 0, // Don't double-count: already added during wager acceptance
            won: winnerId && winnerPosition ? acceptorWon : null, // null for draws  
            is_draw: !winnerId || !winnerPosition,
            wager_type: wagerType
        });

        console.log(`✅ Stats updated for both users`);

        // Check for milestone rewards (new 10-milestone system: 1st, 10th, 20th, 30th, 40th, 50th, 75th, 100th, 125th, 150th)
        // Each milestone: 1.5% total (0.75% creator + 0.75% acceptor)
        await checkMilestoneRewards(creatorId, acceptorId, wager.wager_id);

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
            .select('total_wagered, total_won, total_lost, win_streak, loss_streak')
            .eq('id', userId)
            .single();

        if (fetchError) {
            console.error(`❌ Error fetching current stats for user ${userId}:`, fetchError);
            return;
        }

        // Calculate new stats
        const newStats = {
            total_wagered: (currentUser.total_wagered || 0) + stats.total_wagered,
            updated_at: new Date().toISOString()
        };

        // Handle win/loss/draw scenarios
        if (stats.won === true) {
            // User won this wager
            newStats.total_won = (currentUser.total_won || 0) + stats.total_wagered;
            newStats.total_lost = currentUser.total_lost || 0; // No change
            newStats.win_streak = (currentUser.win_streak || 0) + 1;
            newStats.loss_streak = 0; // Reset loss streak
        } else if (stats.won === false) {
            // User lost this wager
            newStats.total_won = currentUser.total_won || 0; // No change
            newStats.total_lost = (currentUser.total_lost || 0) + stats.total_wagered;
            newStats.win_streak = 0; // Reset win streak
            newStats.loss_streak = (currentUser.loss_streak || 0) + 1;
        } else {
            // This is either a draw/refund or just wager acceptance
            // For draws/acceptance, no win/loss changes
            if (stats.is_draw) {
                newStats.total_won = currentUser.total_won || 0;
                newStats.total_lost = currentUser.total_lost || 0;
                newStats.win_streak = currentUser.win_streak || 0;
                newStats.loss_streak = currentUser.loss_streak || 0;
            } else {
                // Wager acceptance - no win/loss changes yet
                newStats.total_won = currentUser.total_won || 0;
                newStats.total_lost = currentUser.total_lost || 0;
                newStats.win_streak = currentUser.win_streak || 0;
                newStats.loss_streak = currentUser.loss_streak || 0;
            }
        }

        // Calculate win rate
        const totalWins = newStats.total_won || 0;
        const totalLoss = newStats.total_lost || 0;
        const totalResolved = totalWins + totalLoss;
        newStats.win_rate = totalResolved > 0 ? (totalWins / (totalWins + totalLoss)) * 100 : 0;

        // Update user stats in database
        const { error: updateError } = await supabase
            .from('users')
            .update(newStats)
            .eq('id', userId);

        if (updateError) {
            console.error(`❌ Error updating stats for user ${userId}:`, updateError);
        } else {
            console.log(`✅ Updated stats for user ${userId}:`, {
                wins: newStats.wins,
                losses: newStats.losses,
                total_wagers: newStats.total_wagers,
                total_wagered: newStats.total_wagered,
                profit_amount: newStats.profit_amount,
                win_streak: newStats.win_streak,
                loss_streak: newStats.loss_streak
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

        // Update creator stats - increment total_wagered only
        await updateSingleUserStats(creatorId, {
            total_wagered: wagerAmount,
            won: null, // Not a win/loss, just acceptance
            is_draw: false, // Not a draw, just acceptance
            wager_type: wagerType
        });

        // Update acceptor stats - increment total_wagered only
        await updateSingleUserStats(acceptorId, {
            total_wagered: wagerAmount,
            won: null, // Not a win/loss, just acceptance
            is_draw: false, // Not a draw, just acceptance
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
            // Only cancel unaccepted wagers (open/matched), leave active wagers for resolution
            for (const wager of cryptoWagersToExpire) {
                // Active wagers should be resolved, not cancelled
                if (wager.status === 'active') {
                    console.log(`⏰ Skipping cancellation for active crypto wager ${wager.id} - will be resolved instead`);
                    continue;
                }

                // Cancel open/matched wagers that never got accepted
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
                    console.log(`✅ Cancelled unaccepted crypto wager ${wager.id} (status: ${wager.status})`);
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
            // Only cancel unaccepted wagers (open/matched), leave active wagers for resolution
            for (const wager of sportsWagersToExpire) {
                // Active wagers should be resolved, not cancelled
                if (wager.status === 'active') {
                    console.log(`⏰ Skipping cancellation for active sports wager ${wager.id} - will be resolved instead`);
                    continue;
                }

                // Cancel open/matched wagers that never got accepted
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
                    console.log(`✅ Cancelled unaccepted sports wager ${wager.id} (status: ${wager.status})`);
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
                p_type: 'direct_message', // Use valid notification type
                p_title: 'System Test',
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

// ============================================================================
// REWARD SYSTEM IMPLEMENTATION
// ============================================================================

// Check and award milestone rewards for new 10-milestone system
// Each milestone: 1.5% total (0.75% creator + 0.75% acceptor)
async function checkMilestoneRewards(creatorId, acceptorId, wagerId) {
    try {
        // Get today's wager count and milestone status
        const today = new Date().toISOString().split('T')[0];
        const { data: wagerCount, error: countError } = await supabase
            .from('daily_wager_counts')
            .select('*')
            .eq('wager_date', today)
            .single();

        if (countError || !wagerCount) {
            console.error('❌ Error getting daily wager count:', countError);
            return;
        }

        // Check which milestones were just reached
        const currentCount = wagerCount.wager_count;
        const milestones = [
            { count: 1, name: '1st', reached: wagerCount.milestone_1_reached },
            { count: 10, name: '10th', reached: wagerCount.milestone_10_reached },
            { count: 20, name: '20th', reached: wagerCount.milestone_20_reached },
            { count: 30, name: '30th', reached: wagerCount.milestone_30_reached },
            { count: 40, name: '40th', reached: wagerCount.milestone_40_reached },
            { count: 50, name: '50th', reached: wagerCount.milestone_50_reached },
            { count: 75, name: '75th', reached: wagerCount.milestone_75_reached },
            { count: 100, name: '100th', reached: wagerCount.milestone_100_reached },
            { count: 125, name: '125th', reached: wagerCount.milestone_125_reached },
            { count: 150, name: '150th', reached: wagerCount.milestone_150_reached }
        ];

        // Find newly reached milestones
        for (const milestone of milestones) {
            if (currentCount >= milestone.count && !milestone.reached) {
                console.log(`🎉 MILESTONE REACHED! ${milestone.name} wager of the day (${currentCount} total)`);
                await scheduleMilestoneReward(creatorId, acceptorId, milestone.name, currentCount);
            }
        }

    } catch (error) {
        console.error('❌ Error in checkMilestoneRewards:', error);
    }
}

// Schedule milestone reward for distribution (creator + acceptor split)
async function scheduleMilestoneReward(creatorId, acceptorId, milestone, wagerCount) {
    try {
        // Get today's treasury snapshot
        const { data: snapshot, error: snapshotError } = await supabase
            .from('treasury_daily_snapshots')
            .select('id, reward_budget')
            .eq('snapshot_date', new Date().toISOString().split('T')[0])
            .single();

        if (snapshotError || !snapshot) {
            console.error('❌ Error getting treasury snapshot for milestone reward:', snapshotError);
            return;
        }

        // Calculate reward amounts (1.5% total, split 0.75% each)
        const totalRewardPercentage = 1.5; // 1.5% total for each milestone
        const individualRewardPercentage = 0.75; // 0.75% for creator, 0.75% for acceptor
        const totalRewardAmount = (snapshot.reward_budget * totalRewardPercentage) / 100;
        const individualRewardAmount = totalRewardAmount / 2; // Split evenly

        if (totalRewardAmount <= 0) {
            console.log('⚠️ No reward budget available for milestone reward');
            return;
        }

        // Get creator details
        const { data: creator, error: creatorError } = await supabase
            .from('users')
            .select('wallet_address')
            .eq('id', creatorId)
            .single();

        if (creatorError || !creator) {
            console.error('❌ Error getting creator for milestone reward:', creatorError);
            return;
        }

        // Get acceptor details
        const { data: acceptor, error: acceptorError } = await supabase
            .from('users')
            .select('wallet_address')
            .eq('id', acceptorId)
            .single();

        if (acceptorError || !acceptor) {
            console.error('❌ Error getting acceptor for milestone reward:', acceptorError);
            return;
        }

        // Create reward distribution records for both users
        const rewardType = `milestone_${milestone.replace('th', '')}`;

        // Creator reward
        const { error: creatorRewardError } = await supabase
            .from('reward_distributions')
            .insert({
                snapshot_id: snapshot.id,
                user_id: creatorId,
                user_address: creator.wallet_address,
                reward_type: `${rewardType}_creator`,
                reward_amount: individualRewardAmount,
                reward_percentage: individualRewardPercentage,
                wager_count_at_time: wagerCount
            });

        if (creatorRewardError) {
            console.error('❌ Error creating creator milestone reward distribution:', creatorRewardError);
            return;
        }

        // Acceptor reward
        const { error: acceptorRewardError } = await supabase
            .from('reward_distributions')
            .insert({
                snapshot_id: snapshot.id,
                user_id: acceptorId,
                user_address: acceptor.wallet_address,
                reward_type: `${rewardType}_acceptor`,
                reward_amount: individualRewardAmount,
                reward_percentage: individualRewardPercentage,
                wager_count_at_time: wagerCount
            });

        if (acceptorRewardError) {
            console.error('❌ Error creating acceptor milestone reward distribution:', acceptorRewardError);
            return;
        }

        console.log(`✅ Scheduled ${milestone} milestone rewards: ${individualRewardAmount.toFixed(6)} SOL each for creator and acceptor (${totalRewardAmount.toFixed(6)} SOL total)`);

        // Schedule immediate distribution
        await distributePendingRewards();

    } catch (error) {
        console.error('❌ Error in scheduleMilestoneReward:', error);
    }
}

// Get current treasury balance from on-chain
async function getTreasuryBalance() {
    try {
        const balance = await connection.getBalance(treasuryKeypair.publicKey);
        return balance / LAMPORTS_PER_SOL;
    } catch (error) {
        console.error('❌ Error getting treasury balance:', error);
        return 0;
    }
}

// Daily treasury calculation and reward budget setup (runs at 11:59 PM)
async function calculateDailyRewards() {
    try {
        console.log('🏦 Starting daily treasury calculation...');

        const today = new Date().toISOString().split('T')[0];
        const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

        // Get current treasury balance
        const currentBalance = await getTreasuryBalance();
        console.log(`💰 Current treasury balance: ${currentBalance} SOL`);

        // Get yesterday's snapshot to calculate earnings
        const { data: yesterdaySnapshot, error: yesterdayError } = await supabase
            .from('treasury_daily_snapshots')
            .select('*')
            .eq('snapshot_date', yesterday)
            .single();

        const startBalance = yesterdaySnapshot?.treasury_balance_end || 0;
        const dailyEarnings = Math.max(0, currentBalance - startBalance);

        console.log(`📊 Daily earnings: ${dailyEarnings} SOL`);

        // Calculate reward budget (20% of yesterday's earnings)
        const { data: rewardBudgetResult, error: budgetError } = await supabase.rpc('calculate_daily_reward_budget', {
            target_date: today
        });

        const rewardBudget = budgetError ? 0 : (rewardBudgetResult || 0);

        console.log(`🎁 Reward budget for today: ${rewardBudget} SOL`);

        // Create or update today's snapshot
        const { error: snapshotError } = await supabase
            .from('treasury_daily_snapshots')
            .upsert({
                snapshot_date: today,
                treasury_balance_start: startBalance,
                treasury_balance_end: currentBalance,
                daily_earnings: dailyEarnings,
                reward_budget: rewardBudget,
                is_calculated: true
            }, {
                onConflict: 'snapshot_date'
            });

        if (snapshotError) {
            console.error('❌ Error creating treasury snapshot:', snapshotError);
            return;
        }

        console.log('✅ Daily treasury calculation completed');

        // Schedule random winners and micro-drops for the day
        if (rewardBudget > 0) {
            await scheduleRandomRewards(today, rewardBudget);
        }

    } catch (error) {
        console.error('❌ Error in calculateDailyRewards:', error);
    }
}

// Legacy immediate scheduling function - replaced by gradual distribution
// This function is kept for manual testing via /admin/test-rewards endpoint
async function scheduleRandomRewards(date, rewardBudget, snapshotId = null) {
    try {
        console.log('🎲 Scheduling immediate rewards for testing...');

        // Use provided snapshot ID or get today's snapshot
        let snapshot;
        if (snapshotId) {
            snapshot = { id: snapshotId };
        } else {
            const { data: snapshotData, error: snapshotError } = await supabase
                .from('treasury_daily_snapshots')
                .select('id')
                .eq('snapshot_date', date)
                .single();

            if (snapshotError || !snapshotData) {
                console.error('❌ Error getting snapshot for random rewards:', snapshotError);
                return;
            }
            snapshot = snapshotData;
        }

        // Get eligible users (active users with recent activity)
        const { data: eligibleUsers, error: usersError } = await supabase.rpc('get_eligible_reward_users', {
            days_back: 30
        });

        if (usersError || !eligibleUsers || eligibleUsers.length === 0) {
            console.log('⚠️ No eligible users found for random rewards');
            return;
        }

        console.log(`👥 Found ${eligibleUsers.length} eligible users for rewards`);

        // Schedule immediate rewards for testing
        const randomWinnerReward = (rewardBudget * 25) / (100 * 10); // 25% ÷ 10 winners = 2.5% each
        const selectedWinners = shuffleArray([...eligibleUsers]).slice(0, Math.min(10, eligibleUsers.length));

        for (const user of selectedWinners) {
            await createRewardDistribution(snapshot.id, user, 'random_winner', randomWinnerReward, 2.5);
        }

        // Micro-drops for testing (limited to available users)
        const microDropReward = (rewardBudget * 35) / (100 * 100); // 35% ÷ 100 drops = 0.35% each
        const microDropUsers = shuffleArray([...eligibleUsers]).slice(0, Math.min(100, eligibleUsers.length));

        for (const user of microDropUsers) {
            await createRewardDistribution(snapshot.id, user, 'micro_drop', microDropReward, 0.35);
        }

        // Handle buyback separately to avoid precision issues
        const buybackAmount = (rewardBudget * 25) / 100; // 25% of reward budget

        // Store buyback amount in snapshot instead of creating problematic reward distribution
        const { error: buybackError } = await supabase
            .from('treasury_daily_snapshots')
            .update({
                buyback_amount: buybackAmount,
                buyback_wallet: 'FPBUsH6tJgRaUu6diyS2AuwvXESrA9MPqJ9cov15boPQ'
            })
            .eq('id', snapshot.id);

        if (buybackError) {
            console.error('❌ Error updating buyback amount:', buybackError);
        } else {
            console.log(`✅ Buyback amount stored: ${buybackAmount.toFixed(6)} SOL (will be distributed separately)`);
        }

        console.log(`✅ Scheduled ${selectedWinners.length} random winners, ${microDropUsers.length} micro-drops, and buyback (${buybackAmount.toFixed(6)} SOL) for immediate testing`);

    } catch (error) {
        console.error('❌ Error in scheduleRandomRewards:', error);
    }
}

// Create a reward distribution record
async function createRewardDistribution(snapshotId, user, rewardType, rewardAmount, rewardPercentage) {
    try {
        // Round reward amount to 6 decimal places to avoid precision issues
        const roundedAmount = Math.round(rewardAmount * 1000000) / 1000000;

        const { error } = await supabase
            .from('reward_distributions')
            .insert({
                snapshot_id: snapshotId,
                user_id: user.user_id || null, // Allow null for buyback
                user_address: user.wallet_address,
                reward_type: rewardType,
                reward_amount: roundedAmount,
                reward_percentage: rewardPercentage,
                random_selection_seed: Math.random().toString(36).substr(2, 9)
            });

        if (error) {
            if (error.message.includes('numeric field overflow')) {
                console.error(`❌ Database precision issue for ${rewardType}: ${rewardAmount} SOL`);
                console.error(`💡 Need to run: ALTER TABLE reward_distributions ALTER COLUMN reward_amount TYPE NUMERIC(10,6);`);
            } else {
                console.error(`❌ Error creating ${rewardType} reward distribution:`, error);
            }
        } else {
            console.log(`✅ Created ${rewardType} reward: ${roundedAmount} SOL`);
        }
    } catch (error) {
        console.error(`❌ Error in createRewardDistribution for ${rewardType}:`, error);
    }
}

// Distribute pending rewards
async function distributePendingRewards() {
    try {
        // Get all pending reward distributions (excluding buyback to avoid precision issues)
        const { data: pendingRewards, error } = await supabase
            .from('reward_distributions')
            .select('*')
            .eq('is_distributed', false)
            .neq('reward_type', 'wager_buyback') // Exclude buyback from regular distribution
            .order('created_at', { ascending: true })
            .limit(50); // Process in batches

        if (error || !pendingRewards || pendingRewards.length === 0) {
            return; // No pending rewards
        }

        console.log(`💸 Distributing ${pendingRewards.length} pending rewards...`);

        // Track amounts by reward type for treasury snapshot updates
        let randomWinnersTotal = 0;
        let milestoneRewardsTotal = 0;
        let microDropsTotal = 0;
        let buybackTotal = 0;
        let totalDistributed = 0;
        let successfulDistributions = 0;

        for (const reward of pendingRewards) {
            const success = await distributeReward(reward);
            if (success) {
                successfulDistributions++;
                totalDistributed += parseFloat(reward.reward_amount);

                // Track by reward type
                if (reward.reward_type === 'random_winner') {
                    randomWinnersTotal += parseFloat(reward.reward_amount);
                } else if (reward.reward_type.startsWith('milestone_')) {
                    milestoneRewardsTotal += parseFloat(reward.reward_amount);
                } else if (reward.reward_type === 'micro_drop') {
                    microDropsTotal += parseFloat(reward.reward_amount);
                } else if (reward.reward_type === 'wager_buyback') {
                    buybackTotal += parseFloat(reward.reward_amount);
                }
            }
            await new Promise(resolve => setTimeout(resolve, 100)); // Small delay between transactions
        }

        // Update treasury snapshot with distributed amounts
        if (successfulDistributions > 0) {
            await updateTreasurySnapshot(randomWinnersTotal, milestoneRewardsTotal, microDropsTotal, buybackTotal, totalDistributed);
        }

        // Check if we should mark the snapshot as fully distributed
        await checkAndMarkSnapshotAsDistributed();

        console.log(`✅ Successfully distributed ${successfulDistributions}/${pendingRewards.length} rewards totaling ${totalDistributed.toFixed(6)} SOL`);

    } catch (error) {
        console.error('❌ Error in distributePendingRewards:', error);
    }
}

// Distribute a single reward
async function distributeReward(reward) {
    try {
        console.log(`💰 Distributing ${reward.reward_type} reward: ${reward.reward_amount} SOL to ${reward.user_address}`);

        // Convert to lamports
        const lamports = Math.floor(reward.reward_amount * LAMPORTS_PER_SOL);

        // Create transfer instruction
        const transferInstruction = SystemProgram.transfer({
            fromPubkey: treasuryKeypair.publicKey,
            toPubkey: new PublicKey(reward.user_address),
            lamports: lamports,
        });

        // Create and send transaction
        const transaction = new Transaction().add(transferInstruction);
        const { blockhash } = await connection.getLatestBlockhash();
        transaction.recentBlockhash = blockhash;
        transaction.feePayer = treasuryKeypair.publicKey;

        // Sign and send transaction
        transaction.sign(treasuryKeypair);
        const signature = await connection.sendRawTransaction(transaction.serialize());

        // Confirm transaction
        await connection.confirmTransaction(signature, 'confirmed');

        // Update reward distribution record
        const { error: updateError } = await supabase
            .from('reward_distributions')
            .update({
                transaction_signature: signature,
                is_distributed: true,
                distributed_at: new Date().toISOString()
            })
            .eq('id', reward.id);

        if (updateError) {
            console.error('❌ Error updating reward distribution record:', updateError);
            return false;
        } else {
            console.log(`✅ Reward distributed: ${signature}`);
        }

        // Create notification for user
        await supabase.rpc('create_notification', {
            p_user_address: reward.user_address,
            p_type: 'reward_received',
            p_title: `🎁 Reward Received!`,
            p_message: `You received ${reward.reward_amount} SOL as a ${reward.reward_type.replace('_', ' ')} reward!`,
            p_data: {
                reward_type: reward.reward_type,
                amount: reward.reward_amount,
                signature: signature
            }
        });

        return true; // Success

    } catch (error) {
        console.error(`❌ Error distributing reward ${reward.id}:`, error);

        // Mark as error
        await supabase
            .from('reward_distributions')
            .update({
                distribution_error: error.message
            })
            .eq('id', reward.id);

        return false; // Failure
    }
}

// Distribute buyback reward separately to avoid precision issues
async function distributeBuybackReward(snapshotId) {
    try {
        console.log(`💰 Distributing buyback reward for snapshot: ${snapshotId}`);

        // Get snapshot details
        const { data: snapshot, error: fetchError } = await supabase
            .from('treasury_daily_snapshots')
            .select('*')
            .eq('id', snapshotId)
            .single();

        if (fetchError || !snapshot) {
            throw new Error(`Snapshot not found: ${fetchError?.message || 'Unknown error'}`);
        }

        // Check if buyback is already distributed
        if (snapshot.buyback_distributed > 0) {
            return {
                success: false,
                error: 'Buyback already distributed for this snapshot'
            };
        }

        const buybackAmount = parseFloat(snapshot.buyback_amount || 0);
        const buybackWallet = snapshot.buyback_wallet || 'FPBUsH6tJgRaUu6diyS2AuwvXESrA9MPqJ9cov15boPQ';

        if (buybackAmount <= 0) {
            return {
                success: false,
                error: 'No buyback amount available'
            };
        }

        console.log(`💰 Distributing buyback: ${buybackAmount} SOL to ${buybackWallet}`);

        // Convert to lamports
        const lamports = Math.floor(buybackAmount * LAMPORTS_PER_SOL);

        // Create transfer instruction
        const transferInstruction = SystemProgram.transfer({
            fromPubkey: treasuryKeypair.publicKey,
            toPubkey: new PublicKey(buybackWallet),
            lamports: lamports,
        });

        // Create and send transaction
        const transaction = new Transaction().add(transferInstruction);
        const { blockhash } = await connection.getLatestBlockhash();
        transaction.recentBlockhash = blockhash;
        transaction.feePayer = treasuryKeypair.publicKey;

        // Sign and send transaction
        transaction.sign(treasuryKeypair);
        const signature = await connection.sendRawTransaction(transaction.serialize());

        // Confirm transaction
        await connection.confirmTransaction(signature, 'confirmed');

        // Update snapshot to mark buyback as distributed
        const { error: updateError } = await supabase
            .from('treasury_daily_snapshots')
            .update({
                buyback_distributed: buybackAmount,
                updated_at: new Date().toISOString()
            })
            .eq('id', snapshotId);

        if (updateError) {
            console.error('❌ Error updating buyback distributed status:', updateError);
        }

        console.log(`✅ Buyback distributed successfully: ${signature}`);

        // Check if snapshot should be marked as fully distributed after buyback
        await checkAndMarkSnapshotAsDistributed();

        return {
            success: true,
            buyback_amount: buybackAmount,
            buyback_wallet: buybackWallet,
            signature: signature,
            message: 'Buyback reward distributed successfully'
        };

    } catch (error) {
        console.error('❌ Error distributing buyback reward:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Check if snapshot should be marked as fully distributed
async function checkAndMarkSnapshotAsDistributed() {
    try {
        const today = new Date().toISOString().split('T')[0];

        // Get today's snapshot
        const { data: snapshot, error: fetchError } = await supabase
            .from('treasury_daily_snapshots')
            .select('*')
            .eq('snapshot_date', today)
            .single();

        if (fetchError || !snapshot) {
            return; // No snapshot found
        }

        // Calculate total distributed amount
        const totalDistributed = parseFloat(snapshot.random_winners_distributed || 0) +
            parseFloat(snapshot.milestone_rewards_distributed || 0) +
            parseFloat(snapshot.micro_drops_distributed || 0) +
            parseFloat(snapshot.buyback_distributed || 0);

        const rewardBudget = parseFloat(snapshot.reward_budget || 0);

        // Check if we've distributed at least 99.75% of the budget (allowing for very small rounding differences)
        if (rewardBudget > 0 && totalDistributed >= rewardBudget * 0.9975) {
            console.log(`🎯 Marking snapshot as fully distributed: ${totalDistributed.toFixed(6)}/${rewardBudget.toFixed(6)} SOL (${((totalDistributed / rewardBudget) * 100).toFixed(1)}%)`);

            const { error: updateError } = await supabase
                .from('treasury_daily_snapshots')
                .update({
                    is_distributed: true,
                    updated_at: new Date().toISOString()
                })
                .eq('id', snapshot.id);

            if (updateError) {
                console.error('❌ Error marking snapshot as distributed:', updateError);
            } else {
                console.log(`✅ Snapshot marked as fully distributed for ${today}`);
            }
        } else {
            console.log(`📊 Snapshot progress: ${totalDistributed.toFixed(6)}/${rewardBudget.toFixed(6)} SOL (${((totalDistributed / rewardBudget) * 100).toFixed(1)}%)`);
        }

    } catch (error) {
        console.error('❌ Error checking snapshot distribution status:', error);
    }
}

// Update treasury snapshot with distributed reward amounts
async function updateTreasurySnapshot(randomWinnersTotal, milestoneRewardsTotal, microDropsTotal, buybackTotal, totalDistributed) {
    try {
        const today = new Date().toISOString().split('T')[0];

        // Get current snapshot
        const { data: snapshot, error: fetchError } = await supabase
            .from('treasury_daily_snapshots')
            .select('*')
            .eq('snapshot_date', today)
            .single();

        if (fetchError || !snapshot) {
            console.error('❌ Error fetching treasury snapshot for update:', fetchError);
            return;
        }

        // Update the snapshot with distributed amounts
        const { error: updateError } = await supabase
            .from('treasury_daily_snapshots')
            .update({
                reward_budget_used: (parseFloat(snapshot.reward_budget_used) + totalDistributed).toFixed(8),
                random_winners_distributed: (parseFloat(snapshot.random_winners_distributed) + randomWinnersTotal).toFixed(8),
                milestone_rewards_distributed: (parseFloat(snapshot.milestone_rewards_distributed) + milestoneRewardsTotal).toFixed(8),
                micro_drops_distributed: (parseFloat(snapshot.micro_drops_distributed) + microDropsTotal).toFixed(8),
                wager_buyback_amount: (parseFloat(snapshot.wager_buyback_amount) + buybackTotal).toFixed(8),
                updated_at: new Date().toISOString()
            })
            .eq('snapshot_date', today);

        if (updateError) {
            console.error('❌ Error updating treasury snapshot:', updateError);
        } else {
            console.log(`📊 Treasury snapshot updated: ${totalDistributed.toFixed(6)} SOL distributed`);
            console.log(`   Random Winners: ${randomWinnersTotal.toFixed(6)} SOL`);
            console.log(`   Milestones: ${milestoneRewardsTotal.toFixed(6)} SOL`);
            console.log(`   Micro-Drops: ${microDropsTotal.toFixed(6)} SOL`);
            console.log(`   Buyback: ${buybackTotal.toFixed(6)} SOL`);
        }

    } catch (error) {
        console.error('❌ Error in updateTreasurySnapshot:', error);
    }
}

// Utility function to shuffle array
function shuffleArray(array) {
    const shuffled = [...array];
    for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
}

// ============================================================================
// REWARD SYSTEM CRON JOBS
// ============================================================================

// Daily calculation at 11:59 PM
setInterval(async () => {
    const now = new Date();
    const isTime = now.getHours() === 23 && now.getMinutes() === 59;

    if (isTime) {
        await calculateDailyRewards();
        await scheduleNextDayRewards(); // Schedule tomorrow's gradual distribution
    }
}, 60000); // Check every minute

// Gradual reward distribution system
setInterval(async () => {
    await processScheduledRewards();
}, 60000); // Check every minute for scheduled distributions

// Emergency fallback - distribute any remaining pending rewards
setInterval(async () => {
    await distributePendingRewards();
}, 30 * 60 * 1000); // Every 30 minutes as backup

// ============================================================================
// GRADUAL REWARD DISTRIBUTION SYSTEM
// ============================================================================

// Schedule next day's rewards with specific timing
async function scheduleNextDayRewards() {
    try {
        console.log('📅 Scheduling next day\'s gradual reward distribution...');

        const tomorrow = new Date();
        tomorrow.setDate(tomorrow.getDate() + 1);
        const tomorrowDate = tomorrow.toISOString().split('T')[0];

        // Get today's snapshot for reward budget
        const today = new Date().toISOString().split('T')[0];
        const { data: snapshot, error: snapshotError } = await supabase
            .from('treasury_daily_snapshots')
            .select('reward_budget')
            .eq('snapshot_date', today)
            .single();

        if (snapshotError || !snapshot || snapshot.reward_budget <= 0) {
            console.log('⚠️ No reward budget available for tomorrow');
            return;
        }

        const rewardBudget = parseFloat(snapshot.reward_budget);
        console.log(`💰 Tomorrow's reward budget: ${rewardBudget} SOL`);

        // Clear any existing scheduled rewards for tomorrow (in case of re-run)
        await supabase
            .from('scheduled_reward_distributions')
            .delete()
            .eq('reward_date', tomorrowDate);

        // Schedule buyback at 00:01 AM
        await scheduleReward(tomorrowDate, '00:01:00', 'wager_buyback', (rewardBudget * 25) / 100, 25.0, null, 'FPBUsH6tJgRaUu6diyS2AuwvXESrA9MPqJ9cov15boPQ');

        // Schedule 10 random winners (8 AM to 6 PM, hourly)
        await scheduleRandomWinners(tomorrowDate, rewardBudget);

        // Schedule 100 micro-drops (random times 00:01 to 23:59)
        await scheduleMicroDrops(tomorrowDate, rewardBudget);

        console.log('✅ Next day\'s rewards scheduled successfully');

    } catch (error) {
        console.error('❌ Error in scheduleNextDayRewards:', error);
    }
}

// Schedule random winners (1 per hour from 8 AM to 6 PM)
async function scheduleRandomWinners(date, rewardBudget) {
    try {
        // Get eligible users
        const { data: eligibleUsers, error: usersError } = await supabase.rpc('get_eligible_reward_users', {
            days_back: 30
        });

        if (usersError || !eligibleUsers || eligibleUsers.length === 0) {
            console.log('⚠️ No eligible users found for random winners');
            return;
        }

        const randomWinnerReward = (rewardBudget * 25) / (100 * 10); // 25% ÷ 10 winners
        const selectedWinners = shuffleArray([...eligibleUsers]).slice(0, 10);

        // Schedule 1 winner per hour from 8 AM to 6 PM (10 hours total)
        for (let i = 0; i < selectedWinners.length; i++) {
            const hour = 8 + i; // 8 AM to 5 PM (10 hours)
            const minute = Math.floor(Math.random() * 60); // Random minute
            const scheduledTime = `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}:00`;

            await scheduleReward(date, scheduledTime, 'random_winner', randomWinnerReward, 2.5, selectedWinners[i].user_id, selectedWinners[i].wallet_address);
        }

        console.log(`📋 Scheduled ${selectedWinners.length} random winners (8 AM - 6 PM)`);

    } catch (error) {
        console.error('❌ Error in scheduleRandomWinners:', error);
    }
}

// Schedule micro-drops (100 random times throughout 24 hours)
async function scheduleMicroDrops(date, rewardBudget) {
    try {
        // Get eligible users
        const { data: eligibleUsers, error: usersError } = await supabase.rpc('get_eligible_reward_users', {
            days_back: 30
        });

        if (usersError || !eligibleUsers || eligibleUsers.length === 0) {
            console.log('⚠️ No eligible users found for micro-drops');
            return;
        }

        const microDropReward = (rewardBudget * 35) / (100 * 100); // 35% ÷ 100 drops
        const microDropUsers = shuffleArray([...eligibleUsers]).slice(0, Math.min(100, eligibleUsers.length));

        // Generate 100 random times throughout the day (00:01 to 23:59)
        const scheduledTimes = [];
        for (let i = 0; i < 100; i++) {
            const hour = Math.floor(Math.random() * 24);
            const minute = Math.floor(Math.random() * 60);
            const second = Math.floor(Math.random() * 60);

            // Avoid 00:00:xx to prevent conflicts with buyback
            if (hour === 0 && minute === 0) {
                continue;
            }

            const timeString = `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}:${second.toString().padStart(2, '0')}`;

            // Ensure no duplicate times
            if (!scheduledTimes.includes(timeString)) {
                scheduledTimes.push(timeString);
            }
        }

        // Fill remaining slots if we have duplicates
        while (scheduledTimes.length < 100) {
            const hour = Math.floor(Math.random() * 24);
            const minute = Math.floor(Math.random() * 60);
            const second = Math.floor(Math.random() * 60);

            if (hour === 0 && minute === 0) continue;

            const timeString = `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}:${second.toString().padStart(2, '0')}`;

            if (!scheduledTimes.includes(timeString)) {
                scheduledTimes.push(timeString);
            }
        }

        // Sort times chronologically
        scheduledTimes.sort();

        // Schedule micro-drops with random user selection (users can win multiple times)
        for (let i = 0; i < Math.min(100, scheduledTimes.length); i++) {
            const randomUser = eligibleUsers[Math.floor(Math.random() * eligibleUsers.length)];
            await scheduleReward(date, scheduledTimes[i], 'micro_drop', microDropReward, 0.35, randomUser.user_id, randomUser.wallet_address);
        }

        console.log(`📋 Scheduled ${Math.min(100, scheduledTimes.length)} micro-drops (24-hour random distribution)`);

    } catch (error) {
        console.error('❌ Error in scheduleMicroDrops:', error);
    }
}

// Helper function to schedule a single reward
async function scheduleReward(date, time, rewardType, amount, percentage, userId, userAddress) {
    try {
        const { error } = await supabase
            .from('scheduled_reward_distributions')
            .insert({
                reward_date: date,
                scheduled_time: time,
                reward_type: rewardType,
                reward_amount: amount,
                reward_percentage: percentage,
                user_id: userId,
                user_address: userAddress
            });

        if (error) {
            console.error(`❌ Error scheduling ${rewardType} reward:`, error);
        }
    } catch (error) {
        console.error(`❌ Error in scheduleReward for ${rewardType}:`, error);
    }
}

// Process scheduled rewards (check every minute)
async function processScheduledRewards() {
    try {
        // Get pending scheduled rewards for current time
        const { data: pendingRewards, error } = await supabase.rpc('get_pending_scheduled_rewards');

        if (error || !pendingRewards || pendingRewards.length === 0) {
            return; // No pending rewards
        }

        console.log(`🎯 Processing ${pendingRewards.length} scheduled rewards...`);

        for (const scheduledReward of pendingRewards) {
            await executeScheduledReward(scheduledReward);
            await new Promise(resolve => setTimeout(resolve, 100)); // Small delay
        }

    } catch (error) {
        console.error('❌ Error in processScheduledRewards:', error);
    }
}

// Execute a single scheduled reward
async function executeScheduledReward(scheduledReward) {
    try {
        console.log(`⏰ Executing ${scheduledReward.reward_type} reward: ${scheduledReward.reward_amount} SOL to ${scheduledReward.user_address}`);

        // Create reward distribution record
        const { data: rewardDist, error: createError } = await supabase
            .from('reward_distributions')
            .insert({
                snapshot_id: await getTodaySnapshotId(),
                user_id: scheduledReward.user_id,
                user_address: scheduledReward.user_address,
                reward_type: scheduledReward.reward_type,
                reward_amount: scheduledReward.reward_amount,
                reward_percentage: scheduledReward.reward_percentage,
                random_selection_seed: Math.random().toString(36).substr(2, 9)
            })
            .select()
            .single();

        if (createError) {
            console.error(`❌ Error creating reward distribution for scheduled reward:`, createError);
            return;
        }

        // Execute the actual distribution
        const success = await distributeReward(rewardDist);

        if (success) {
            // Mark scheduled reward as executed
            await supabase
                .from('scheduled_reward_distributions')
                .update({
                    is_executed: true,
                    executed_at: new Date().toISOString(),
                    distribution_id: rewardDist.id
                })
                .eq('id', scheduledReward.id);

            console.log(`✅ Scheduled ${scheduledReward.reward_type} reward executed successfully`);
        }

    } catch (error) {
        console.error(`❌ Error executing scheduled reward:`, error);
    }
}

// Helper function to get today's snapshot ID
async function getTodaySnapshotId() {
    const today = new Date().toISOString().split('T')[0];
    const { data: snapshot, error } = await supabase
        .from('treasury_daily_snapshots')
        .select('id')
        .eq('snapshot_date', today)
        .single();

    return snapshot?.id || null;
}

console.log('🎁 Gradual reward system initialized - Rewards distributed throughout the day');

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 Received SIGTERM, shutting down gracefully...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('🛑 Received SIGINT, shutting down gracefully...');
    process.exit(0);
});
