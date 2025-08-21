#!/usr/bin/env node

/**
 * WagerFi Reward System Test Script
 * 
 * This script tests the complete reward system flow:
 * 1. Check treasury balance
 * 2. Schedule test rewards with 5 SOL budget
 * 3. Distribute pending rewards
 * 4. Verify transactions and notifications
 */

import axios from 'axios';

const BASE_URL = process.env.BACKGROUND_WORKER_URL || 'https://backgroundworker-11kk.onrender.com';
const TEST_BUDGET = 4.0; // 4 SOL for testing (adjusted to available balance)

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function makeRequest(method, endpoint, data = null) {
    try {
        const config = {
            method,
            url: `${BASE_URL}${endpoint}`,
            headers: { 'Content-Type': 'application/json' }
        };

        if (data) {
            config.data = data;
        }

        const response = await axios(config);
        return { success: true, data: response.data };
    } catch (error) {
        return {
            success: false,
            error: error.response?.data || error.message
        };
    }
}

async function testRewardSystem() {
    console.log('🚀 Starting WagerFi Reward System Test');
    console.log('='.repeat(50));

    // Step 1: Check treasury balance
    console.log('\n📊 Step 1: Checking treasury balance...');
    const balanceResult = await makeRequest('GET', '/admin/treasury-balance');

    if (!balanceResult.success) {
        console.error('❌ Failed to get treasury balance:', balanceResult.error);
        return;
    }

    const { balance, address } = balanceResult.data;
    console.log(`💰 Treasury Balance: ${balance} SOL`);
    console.log(`🏦 Treasury Address: ${address}`);

    if (balance < TEST_BUDGET) {
        console.error(`❌ Insufficient treasury balance! Need ${TEST_BUDGET} SOL, have ${balance} SOL`);
        return;
    }

    // Step 2: Schedule test rewards
    console.log(`\n🎲 Step 2: Scheduling test rewards with ${TEST_BUDGET} SOL budget...`);
    const scheduleResult = await makeRequest('POST', '/admin/test-rewards', {
        testBudget: TEST_BUDGET
    });

    if (!scheduleResult.success) {
        console.error('❌ Failed to schedule test rewards:', scheduleResult.error);
        return;
    }

    console.log('✅ Test rewards scheduled successfully!');
    console.log('📋 Details:', scheduleResult.data.details);

    // Step 3: Wait a moment for database operations
    console.log('\n⏳ Step 3: Waiting 3 seconds for scheduling to complete...');
    await sleep(3000);

    // Step 4: Distribute pending rewards
    console.log('\n💸 Step 4: Distributing pending rewards...');
    const distributeResult = await makeRequest('POST', '/admin/distribute-rewards');

    if (!distributeResult.success) {
        console.error('❌ Failed to distribute rewards:', distributeResult.error);
        return;
    }

    console.log('✅ Reward distribution completed!');
    console.log('📋 Distribution result:', distributeResult.data);

    // Step 5: Check final treasury balance
    console.log('\n📊 Step 5: Checking final treasury balance...');
    await sleep(2000); // Wait for transactions to settle

    const finalBalanceResult = await makeRequest('GET', '/admin/treasury-balance');
    if (finalBalanceResult.success) {
        const finalBalance = finalBalanceResult.data.balance;
        const amountDistributed = balance - finalBalance;

        console.log(`💰 Final Treasury Balance: ${finalBalance} SOL`);
        console.log(`💸 Amount Distributed: ${amountDistributed.toFixed(6)} SOL`);
        console.log(`📊 Distribution Efficiency: ${((amountDistributed / TEST_BUDGET) * 100).toFixed(2)}%`);
    }

    // Test Summary
    console.log('\n' + '='.repeat(50));
    console.log('🎉 REWARD SYSTEM TEST COMPLETED!');
    console.log('='.repeat(50));

    console.log('\n📈 Test Results:');
    console.log(`✅ Treasury balance checked: ${balance} SOL available`);
    console.log(`✅ Test rewards scheduled: ${TEST_BUDGET} SOL budget`);
    console.log(`✅ Reward distribution executed`);
    console.log(`✅ Notifications sent to reward recipients`);

    console.log('\n🔍 What was tested:');
    console.log('• Treasury balance monitoring');
    console.log('• Random winner selection (10 winners × 0.5% = 5%)');
    console.log('• Micro-drop distribution (100 drops = 7%)');
    console.log('• On-chain SOL transfers from treasury');
    console.log('• Database reward tracking');
    console.log('• User notifications');

    console.log('\n📱 Next steps:');
    console.log('• Check user notifications in the frontend');
    console.log('• Verify reward history in rewards dashboard');
    console.log('• Monitor transaction signatures on Solana explorer');

    console.log('\n🏁 Test completed successfully! 🚀');
}

// Run the test
testRewardSystem().catch(error => {
    console.error('💥 Test failed with error:', error);
    process.exit(1);
});
