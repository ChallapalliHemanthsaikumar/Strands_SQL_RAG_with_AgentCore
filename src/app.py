

from agent.custom_agent import sql_agent


if __name__ == "__main__":
    print("🤖 Strands SQL RAG Agent")
    print("Type 'quit' to exit")
    print("-" * 30)
    
    while True:
        query = input("\n👤 Enter your query: ").strip()
        
        if query.lower() in ['quit', 'exit', 'q']:
            print("👋 Goodbye!")
            break
            
        if not query:
            print("⚠️ Please enter a valid query")
            continue
            
        try:
            response = sql_agent(user_query=query)
            print(f"\n🤖 Response: {response}")
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            print("Please try again or type 'quit' to exit")

