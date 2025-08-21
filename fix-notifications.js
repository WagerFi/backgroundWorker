import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
);

console.log('🔧 Fixing notification function...');

const fixFunction = `
-- Update the create_notification function to include user_address
CREATE OR REPLACE FUNCTION create_notification(
    p_user_address TEXT,
    p_type TEXT,
    p_title TEXT,
    p_message TEXT,
    p_data JSONB DEFAULT '{}'::jsonb
)
RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    notification_id UUID;
    user_record RECORD;
BEGIN
    -- Get user record
    SELECT id, wallet_address INTO user_record
    FROM users 
    WHERE wallet_address = p_user_address;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'User not found with wallet address: %', p_user_address;
    END IF;
    
    -- Validate notification type
    IF p_type NOT IN ('wager_matched', 'wager_resolved', 'wager_expired', 'referral_signup', 'level_up', 'reward_received', 'friend_request', 'direct_message') THEN
        RAISE EXCEPTION 'Invalid notification type: %', p_type;
    END IF;
    
    -- Insert notification with user_address populated
    INSERT INTO notifications (user_id, user_address, type, title, message, data)
    VALUES (user_record.id, user_record.wallet_address, p_type, p_title, p_message, p_data)
    RETURNING id INTO notification_id;
    
    RETURN notification_id;
END;
$$;
`;

try {
    const { error } = await supabase.rpc('exec', { sql: fixFunction });

    if (error) {
        console.error('❌ Failed to fix notification function:', error);
    } else {
        console.log('✅ Notification function fixed successfully!');
    }
} catch (err) {
    console.error('❌ Error:', err.message);
}
