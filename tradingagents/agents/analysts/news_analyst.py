from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import time
import json


def create_news_analyst(llm, toolkit):
    def news_analyst_node(state):
        current_date = state["trade_date"]
        ticker = state["company_of_interest"]
        market_region = state.get(
            "market_region",
            toolkit.config.get("market_region", "cn_a"),
        )

        if market_region == "cn_a":
            tools = [toolkit.get_a_share_company_news]
        elif toolkit.config["online_tools"]:
            tools = [toolkit.get_global_news_openai, toolkit.get_google_news]
        else:
            tools = [
                toolkit.get_finnhub_news,
                toolkit.get_reddit_news,
                toolkit.get_google_news,
            ]

        chinese_output_requirement = (
            "请默认使用简体中文输出完整报告。除股票代码、公司英文名、模型名、必要英文缩写"
            "（如 PE、PS、EPS、BUY/HOLD/SELL）外，其余标题、段落、表格列名、总结和结论都请使用中文。"
        )

        if market_region == "cn_a":
            system_message = (
                "You are a CN A-share news researcher. Focus on company announcements, company events, broker research, and policy-sensitive industry developments that materially affect the target stock over the past week. Prioritize company-specific evidence over generic overseas macro headlines, and explain why the news flow matters for near-term and medium-term trading."
                + """ Make sure to append a Makrdown table at the end of the report to organize key points in the report, organized and easy to read."""
                + chinese_output_requirement
            )
        else:
            system_message = (
                "You are a news researcher tasked with analyzing recent news and trends over the past week. Please write a comprehensive report of the current state of the world that is relevant for trading and macroeconomics. Look at news from EODHD, and finnhub to be comprehensive. Do not simply state the trends are mixed, provide detailed and finegrained analysis and insights that may help traders make decisions."
                + """ Make sure to append a Makrdown table at the end of the report to organize key points in the report, organized and easy to read."""
                + chinese_output_requirement
            )

        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You are a helpful AI assistant, collaborating with other assistants."
                    " Use the provided tools to progress towards answering the question."
                    " If you are unable to fully answer, that's OK; another assistant with different tools"
                    " will help where you left off. Execute what you can to make progress."
                    " If you or any other assistant has the FINAL TRANSACTION PROPOSAL: **BUY/HOLD/SELL** or deliverable,"
                    " prefix your response with FINAL TRANSACTION PROPOSAL: **BUY/HOLD/SELL** so the team knows to stop."
                    " You have access to the following tools: {tool_names}.\n{system_message}"
                    "For your reference, the current date is {current_date}. We are looking at the company {ticker}",
                ),
                MessagesPlaceholder(variable_name="messages"),
            ]
        )

        prompt = prompt.partial(system_message=system_message)
        prompt = prompt.partial(tool_names=", ".join([tool.name for tool in tools]))
        prompt = prompt.partial(current_date=current_date)
        prompt = prompt.partial(ticker=ticker)

        chain = prompt | llm.bind_tools(tools)
        result = chain.invoke(state["messages"])

        report = ""

        if len(result.tool_calls) == 0:
            report = result.content

        return {
            "messages": [result],
            "news_report": report,
        }

    return news_analyst_node
