import functools


def create_trader(llm, memory):
    def trader_node(state, name):
        company_name = state["company_of_interest"]
        investment_plan = state["investment_plan"]
        market_research_report = state["market_report"]
        sentiment_report = state["sentiment_report"]
        news_report = state["news_report"]
        fundamentals_report = state["fundamentals_report"]
        structured_decision = state.get("structured_decision", {})

        curr_situation = (
            f"{market_research_report}\n\n{sentiment_report}\n\n"
            f"{news_report}\n\n{fundamentals_report}"
        )
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        if past_memories:
            past_memory_str = "".join(
                record["recommendation"] + "\n\n" for record in past_memories
            )
        else:
            past_memory_str = "No past memories found."

        context = {
            "role": "user",
            "content": (
                f"Based on the research team's work, here is an investment plan for {company_name}.\n\n"
                f"Proposed investment plan:\n{investment_plan}\n\n"
                "Structured reference:\n"
                f"- Suggested action: {structured_decision.get('decision', '')}\n"
                f"- Summary: {structured_decision.get('summary', '')}\n"
                f"- Drivers: {structured_decision.get('primary_drivers', [])}\n"
                f"- Risks: {structured_decision.get('risk_flags', [])}\n\n"
                "Use the structured reference as your baseline, but adjust the execution stance if the debate implies a better practical action."
            ),
        }

        messages = [
            {
                "role": "system",
                "content": (
                "You are a trading agent converting research into an execution-ready recommendation. "
                "Treat the structured factor conclusion as a strong baseline, not a hard lock. "
                "If you lean away from it, explain why the live debate or risk context justifies that change. "
                "Make the plan practical with position logic, risks, and monitoring points. "
                "Default to Simplified Chinese in your output. Keep ticker symbols, company English names, model names, and "
                "the final BUY/HOLD/SELL token in English, but write the rest of the recommendation in Chinese. "
                "End with 'FINAL TRANSACTION PROPOSAL: **BUY/HOLD/SELL**'. "
                f"Past lessons: {past_memory_str}"
            ),
            },
            context,
        ]

        result = llm.invoke(messages)

        return {
            "messages": [result],
            "trader_investment_plan": result.content,
            "sender": name,
        }

    return functools.partial(trader_node, name="Trader")
