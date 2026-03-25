def create_research_manager(llm, memory):
    def research_manager_node(state) -> dict:
        history = state["investment_debate_state"].get("history", "")
        market_research_report = state["market_report"]
        sentiment_report = state["sentiment_report"]
        news_report = state["news_report"]
        fundamentals_report = state["fundamentals_report"]
        investment_debate_state = state["investment_debate_state"]
        structured_decision = state.get("structured_decision", {})

        curr_situation = (
            f"{market_research_report}\n\n{sentiment_report}\n\n"
            f"{news_report}\n\n{fundamentals_report}"
        )
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        past_memory_str = ""
        for record in past_memories:
            past_memory_str += record["recommendation"] + "\n\n"

        prompt = f"""As the portfolio manager and debate facilitator, evaluate this debate round and produce a trader-ready investment plan.

You are also given a structured factor conclusion. Treat it as a strong reference frame, but not an unbreakable rule.

Structured reference:
- Suggested action: {structured_decision.get("decision", "")}
- Summary: {structured_decision.get("summary", "")}
- Primary drivers: {structured_decision.get("primary_drivers", [])}
- Primary risks: {structured_decision.get("risk_flags", [])}

Instructions:
1. Use the structured conclusion as your starting point.
2. If the live debate materially contradicts it, call that out explicitly.
3. Keep the plan practical for the trader, including execution conditions and invalidation signals.
4. Do not force agreement when the evidence is mixed; surface uncertainty honestly.
5. Default to Simplified Chinese in your output. Keep ticker symbols, company English names, model names, and necessary English abbreviations such as BUY/HOLD/SELL in English, but write the rest of the plan in Chinese.

Your output should include:
- a directional plan for the trader
- the strongest arguments from both sides of the debate
- execution conditions or monitoring points
- key caveats that could weaken the thesis

Take into account your past mistakes on similar situations and use them to refine the plan.

Past reflections:
{past_memory_str}

Debate history:
{history}"""

        response = llm.invoke(prompt)

        new_investment_debate_state = {
            "judge_decision": response.content,
            "history": investment_debate_state.get("history", ""),
            "bear_history": investment_debate_state.get("bear_history", ""),
            "bull_history": investment_debate_state.get("bull_history", ""),
            "latest_speaker": "Judge",
            "current_response": response.content,
            "count": investment_debate_state["count"],
        }

        return {
            "investment_debate_state": new_investment_debate_state,
            "investment_plan": response.content,
        }

    return research_manager_node
