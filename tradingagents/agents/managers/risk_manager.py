def create_risk_manager(llm, memory):
    def risk_manager_node(state) -> dict:
        history = state["risk_debate_state"]["history"]
        risk_debate_state = state["risk_debate_state"]
        market_research_report = state["market_report"]
        news_report = state["news_report"]
        fundamentals_report = state["fundamentals_report"]
        sentiment_report = state["sentiment_report"]
        trader_plan = state["trader_investment_plan"]
        structured_decision = state.get("structured_decision", {})

        curr_situation = (
            f"{market_research_report}\n\n{sentiment_report}\n\n"
            f"{news_report}\n\n{fundamentals_report}"
        )
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        past_memory_str = ""
        for record in past_memories:
            past_memory_str += record["recommendation"] + "\n\n"

        prompt = f"""As the Risk Management Judge and final decision editor, evaluate the debate among the risky, neutral, and conservative analysts and produce the final trade note.

You are given a structured factor conclusion as a strong reference, not an absolute rule.

Structured reference:
- Suggested action: {structured_decision.get("decision", "")}
- Summary: {structured_decision.get("summary", "")}
- Primary drivers: {structured_decision.get("primary_drivers", [])}
- Primary risks: {structured_decision.get("risk_flags", [])}
- Threshold policy: {structured_decision.get("threshold_policy", {})}

Decision policy:
1. Start from the structured conclusion as your default anchor.
2. You may keep or override that action if the trader plan and risk debate provide materially stronger contrary evidence.
3. If you override it, clearly state why the override is justified.
4. Use past mistakes to avoid repeating weak BUY/SELL/HOLD calls.
5. Start from the trader's current plan: {trader_plan}
6. Default to Simplified Chinese in your output. Keep ticker symbols, company English names, model names, and the explicit action line BUY/HOLD/SELL in English, but write all reasoning, bullets, and guidance in Chinese.

Return a concise but actionable note with:
- decision rationale
- key bullish points
- key risks
- execution or monitoring guidance
- a final explicit line in the form: FINAL ACTION: BUY/HOLD/SELL

Past reflections:
{past_memory_str}

Analysts debate history:
{history}"""

        response = llm.invoke(prompt)

        new_risk_debate_state = {
            "judge_decision": response.content,
            "history": risk_debate_state["history"],
            "risky_history": risk_debate_state["risky_history"],
            "safe_history": risk_debate_state["safe_history"],
            "neutral_history": risk_debate_state["neutral_history"],
            "latest_speaker": "Judge",
            "current_risky_response": risk_debate_state["current_risky_response"],
            "current_safe_response": risk_debate_state["current_safe_response"],
            "current_neutral_response": risk_debate_state["current_neutral_response"],
            "count": risk_debate_state["count"],
        }

        return {
            "risk_debate_state": new_risk_debate_state,
            "final_trade_decision": response.content,
        }

    return risk_manager_node
