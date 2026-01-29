import asyncio
import os
import sys

# Add local venv to path if needed (though running with venv python should suffice)
# ...

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

SERVER_PATH = r"c:\Users\Fosilio\.gemini\antigravity\scratch\ecom_bi_project\powerbi-mcp\extension\server\powerbi-modeling-mcp.exe"

async def main():
    print(f"Connecting to server at: {SERVER_PATH}")
    
    server_params = StdioServerParameters(
        command=SERVER_PATH,
        args=["--start"], 
        env=os.environ.copy()
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            print("\n--- Connected to Power BI MCP Server ---")
            
            candidates = [
                {"name": "Postgres DB", "str": "localhost:5432"},
                {"name": "Power BI (Raw)", "str": "localhost:49358"},
                {"name": "Power BI (Data Source)", "str": "Data Source=localhost:49358"},
                {"name": "Power BI (Provider)", "str": "Provider=MSOLAP;Data Source=localhost:49358;Initial Catalog=;"}
            ]

            for cand in candidates:
                print(f"\n[Testing connection: {cand['name']} -> {cand['str']}]")
                try:
                    await session.call_tool("connection_operations", arguments={"Operation": "Connect", "ConnectionString": cand['str']})
                    print(f"SUCCESS: Connected to {cand['name']}")
                    break
                except Exception as e:
                    print(f"FAILED: {e}")
            
            # Continue to analysis if any connection succeeded... (omitted for brevity, just testing connection now)

            # 3. Analyze Model
            print("\n[Analysis: Model Stats]")
            model_stats = await session.call_tool("model_operations", arguments={"Operation": "GetStats"})
            print(model_stats.content[0].text)
            
            # 4. List Tables
            print("\n[Analysis: Tables]")
            tables = await session.call_tool("table_operations", arguments={"Operation": "List"})
            print(tables.content[0].text)

            # 5. List Relationships
            print("\n[Analysis: Relationships]")
            rels = await session.call_tool("relationship_operations", arguments={"Operation": "List"})
            print(rels.content[0].text)
            
            print("\nAnalysis Complete.")

if __name__ == "__main__":
    asyncio.run(main())
