import asyncio
import discord
from listener4 import TOKENS, MultiAccountClient

async def get_account_servers(token, index):
    # We use a custom subclass or the original to just fetch data
    client = discord.Client() 

    @client.event
    async def on_ready():
        print(f"\n--- 🧱 Account [{index}] Report ---")
        print(f"👤 Username: {client.user}")
        print(f"🆔 User ID:  {client.user.id}")
        print(f"🌐 Total Servers: {len(client.guilds)}")
        
        print(f"{'Server Name':<30} | {'Server ID':<20} | {'Members':<10}")
        print("-" * 65)
        
        for guild in client.guilds:
            # We truncate long names for a clean table
            name = (guild.name[:27] + '..') if len(guild.name) > 27 else guild.name
            print(f"{name:<30} | {guild.id:<20} | {guild.member_count:<10}")
            
        print(f"--- End of Account [{index}] ---\n")
        await client.close()

    try:
        # Start the client
        await client.start(token)
    except Exception as e:
        print(f"❌ Account [{index}] failed: {e}")

async def main():
    print("🚀 Starting Multi-Account Server Discovery...")
    tasks = []
    for i, token in enumerate(TOKENS):
        if token:
            tasks.append(get_account_servers(token, i+1))
    
    # Run all discovery tasks in parallel
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass