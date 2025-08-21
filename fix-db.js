import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
    'https://ajqhnbzfgihlxkjjawdk.supabase.co',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFqcWhuYnpmZ2lobHhramphd2RrIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTUwNTE5NSwiZXhwIjoyMDcxMDgxMTk1fQ.BjU9hCXmk95mMzMzeGo0yCs0uU2enEfVX_nKswypIaQ'
);

console.log('🔧 Running direct SQL fix...');

// First, update existing notifications to populate user_address
const updateExisting = `
UPDATE notifications 
SET user_address = users.wallet_address
FROM users 
WHERE notifications.user_id = users.id 
  AND notifications.user_address IS NULL;
`;

console.log('📝 Updating existing notifications...');
const updateResult = await supabase.rpc('exec', { sql: updateExisting });
console.log('Update result:', updateResult);

// Then recreate the function
const recreateFunction = `
DROP FUNCTION IF EXISTS create_notification(text,text,text,text,jsonb);

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

console.log('🔧 Recreating function...');
const functionResult = await supabase.rpc('exec', { sql: recreateFunction });
console.log('Function result:', functionResult);

console.log('✅ Database fix completed!');
