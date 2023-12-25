-- Create the trigger to activate the function on INSERT and UPDATE
CREATE TRIGGER subscription_changes_trigger
AFTER INSERT OR UPDATE ON bronze.subscriptions
FOR EACH ROW EXECUTE FUNCTION handle_subscription_changes();