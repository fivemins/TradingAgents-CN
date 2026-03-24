# TradingAgents/graph/trading_graph.py

import os
from pathlib import Path
import json
from datetime import date
from typing import Dict, Any, Tuple, List, Optional

from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI

from langgraph.prebuilt import ToolNode

from tradingagents.agents import *
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.agents.utils.memory import FinancialSituationMemory
from tradingagents.agents.utils.agent_states import (
    AgentState,
    InvestDebateState,
    RiskDebateState,
)
from tradingagents.dataflows.interface import set_config
from tradingagents.provider_utils import (
    OPENAI_COMPATIBLE_PROVIDERS,
    get_llm_api_key,
    get_llm_base_url,
)
from tradingagents.market_utils import build_security_profile
from tradingagents.structured_snapshot import build_structured_analysis

from .conditional_logic import ConditionalLogic
from .setup import GraphSetup
from .propagation import Propagator
from .reflection import Reflector
from .signal_processing import SignalProcessor


class TradingAgentsGraph:
    """Main class that orchestrates the trading agents framework."""

    def __init__(
        self,
        selected_analysts=["market", "social", "news", "fundamentals"],
        debug=False,
        config: Dict[str, Any] = None,
    ):
        """Initialize the trading agents graph and components.

        Args:
            selected_analysts: List of analyst types to include
            debug: Whether to run in debug mode
            config: Configuration dictionary. If None, uses default config
        """
        self.debug = debug
        self.config = config or DEFAULT_CONFIG

        # Update the interface's config
        set_config(self.config)

        # Create necessary directories
        os.makedirs(
            os.path.join(self.config["project_dir"], "dataflows/data_cache"),
            exist_ok=True,
        )

        # Initialize LLMs
        llm_provider = self.config["llm_provider"].lower()
        llm_base_url = get_llm_base_url(self.config)
        llm_api_key = get_llm_api_key(self.config)

        if llm_provider in OPENAI_COMPATIBLE_PROVIDERS:
            openai_kwargs = {"base_url": llm_base_url}
            if llm_api_key:
                openai_kwargs["api_key"] = llm_api_key
            self.deep_thinking_llm = ChatOpenAI(
                model=self.config["deep_think_llm"], **openai_kwargs
            )
            self.quick_thinking_llm = ChatOpenAI(
                model=self.config["quick_think_llm"], **openai_kwargs
            )
        elif llm_provider == "anthropic":
            anthropic_kwargs = {"base_url": llm_base_url}
            if llm_api_key:
                anthropic_kwargs["api_key"] = llm_api_key
            self.deep_thinking_llm = ChatAnthropic(
                model=self.config["deep_think_llm"], **anthropic_kwargs
            )
            self.quick_thinking_llm = ChatAnthropic(
                model=self.config["quick_think_llm"], **anthropic_kwargs
            )
        elif llm_provider == "google":
            google_kwargs = {}
            if llm_api_key:
                google_kwargs["google_api_key"] = llm_api_key
            self.deep_thinking_llm = ChatGoogleGenerativeAI(
                model=self.config["deep_think_llm"], **google_kwargs
            )
            self.quick_thinking_llm = ChatGoogleGenerativeAI(
                model=self.config["quick_think_llm"], **google_kwargs
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {self.config['llm_provider']}")
        
        self.toolkit = Toolkit(config=self.config)

        # Initialize memories
        self.bull_memory = FinancialSituationMemory("bull_memory", self.config)
        self.bear_memory = FinancialSituationMemory("bear_memory", self.config)
        self.trader_memory = FinancialSituationMemory("trader_memory", self.config)
        self.invest_judge_memory = FinancialSituationMemory("invest_judge_memory", self.config)
        self.risk_manager_memory = FinancialSituationMemory("risk_manager_memory", self.config)

        # Create tool nodes
        self.tool_nodes = self._create_tool_nodes()

        # Initialize components
        self.conditional_logic = ConditionalLogic(
            max_debate_rounds=int(self.config["max_debate_rounds"]),
            max_risk_discuss_rounds=int(self.config["max_risk_discuss_rounds"]),
        )
        self.graph_setup = GraphSetup(
            self.quick_thinking_llm,
            self.deep_thinking_llm,
            self.toolkit,
            self.tool_nodes,
            self.bull_memory,
            self.bear_memory,
            self.trader_memory,
            self.invest_judge_memory,
            self.risk_manager_memory,
            self.conditional_logic,
            self.create_structured_decision_node(),
        )

        self.propagator = Propagator(
            max_recur_limit=int(self.config["max_recur_limit"]),
            market_region=str(self.config.get("market_region", "cn_a")),
        )
        self.reflector = Reflector(self.quick_thinking_llm)
        self.signal_processor = SignalProcessor(self.quick_thinking_llm)

        # State tracking
        self.curr_state = None
        self.ticker = None
        self.log_states_dict = {}  # date to full state dict

        # Set up the graph
        self.graph = self.graph_setup.setup_graph(selected_analysts)

    def create_structured_decision_node(self):
        """Create a graph node that snapshots factor scores before debate starts."""

        def structured_decision_node(state: Dict[str, Any]) -> Dict[str, Any]:
            structured_payload = build_structured_analysis(state, self.config)
            return structured_payload

        return structured_decision_node

    def _create_tool_nodes(self) -> Dict[str, ToolNode]:
        """Create tool nodes for different data sources."""
        return {
            "market": ToolNode(
                [
                    # online tools
                    self.toolkit.get_YFin_data_online,
                    self.toolkit.get_technical_indicators_report_online,
                    # offline tools
                    self.toolkit.get_YFin_data,
                    self.toolkit.get_technical_indicators_report,
                ]
            ),
            "social": ToolNode(
                [
                    # online tools
                    self.toolkit.get_stock_news_openai,
                    self.toolkit.get_a_share_company_sentiment,
                    # offline tools
                    self.toolkit.get_reddit_stock_info,
                ]
            ),
            "news": ToolNode(
                [
                    # online tools
                    self.toolkit.get_global_news_openai,
                    self.toolkit.get_a_share_company_news,
                    self.toolkit.get_google_news,
                    # offline tools
                    self.toolkit.get_finnhub_news,
                    self.toolkit.get_reddit_news,
                ]
            ),
            "fundamentals": ToolNode(
                [
                    # online tools
                    self.toolkit.get_fundamentals_openai,
                    self.toolkit.get_a_share_company_fundamentals,
                    # offline tools
                    self.toolkit.get_finnhub_company_insider_sentiment,
                    self.toolkit.get_finnhub_company_insider_transactions,
                    self.toolkit.get_simfin_balance_sheet,
                    self.toolkit.get_simfin_cashflow,
                    self.toolkit.get_simfin_income_stmt,
                ]
            ),
        }

    def propagate(self, company_name, trade_date):
        """Run the trading agents graph for a company on a specific date."""

        self.ticker = company_name

        # Initialize state
        init_agent_state = self.propagator.create_initial_state(
            company_name, trade_date
        )
        args = self.propagator.get_graph_args()

        if self.debug:
            # Debug mode with tracing
            trace = []
            for chunk in self.graph.stream(init_agent_state, **args):
                if len(chunk["messages"]) == 0:
                    pass
                else:
                    chunk["messages"][-1].pretty_print()
                    trace.append(chunk)

            final_state = trace[-1]
        else:
            # Standard mode without tracing
            final_state = self.graph.invoke(init_agent_state, **args)

        final_state = self.enrich_final_state(final_state, company_name, trade_date)

        # Store current state for reflection
        self.curr_state = final_state

        # Log state
        self._log_state(trade_date, final_state)

        # Return decision and processed signal
        return final_state, self.process_signal(
            final_state["final_trade_decision"],
            final_state.get("structured_decision"),
        )

    def enrich_final_state(
        self,
        final_state: Dict[str, Any],
        company_name: str,
        trade_date: str,
    ) -> Dict[str, Any]:
        """Attach normalized identifiers and structured factor snapshots."""
        market_region = final_state.get("market_region") or self.config.get(
            "market_region", "cn_a"
        )
        security_profile = build_security_profile(company_name, market_region)
        final_state["company_of_interest"] = security_profile.normalized_ticker
        final_state["trade_date"] = str(trade_date)
        final_state["market_region"] = security_profile.market_region
        final_state["security_profile"] = security_profile.to_dict()

        structured_payload = build_structured_analysis(final_state, self.config)
        final_state.update(structured_payload)
        final_signal = final_state.get("final_trade_decision")
        if final_signal:
            final_state["final_trade_decision"] = self.signal_processor.rewrite_signal(
                final_signal,
                final_state.get("structured_decision"),
            )
        return final_state

    def _log_state(self, trade_date, final_state):
        """Log the final state to a JSON file."""
        self.log_states_dict[str(trade_date)] = {
            "company_of_interest": final_state["company_of_interest"],
            "trade_date": final_state["trade_date"],
            "market_region": final_state.get("market_region", ""),
            "security_profile": final_state.get("security_profile", {}),
            "market_report": final_state["market_report"],
            "sentiment_report": final_state["sentiment_report"],
            "news_report": final_state["news_report"],
            "fundamentals_report": final_state["fundamentals_report"],
            "factor_snapshot": final_state.get("factor_snapshot", {}),
            "evidence_snapshot": final_state.get("evidence_snapshot", {}),
            "structured_decision": final_state.get("structured_decision", {}),
            "investment_debate_state": {
                "bull_history": final_state["investment_debate_state"]["bull_history"],
                "bear_history": final_state["investment_debate_state"]["bear_history"],
                "history": final_state["investment_debate_state"]["history"],
                "current_response": final_state["investment_debate_state"][
                    "current_response"
                ],
                "judge_decision": final_state["investment_debate_state"][
                    "judge_decision"
                ],
            },
            "trader_investment_decision": final_state["trader_investment_plan"],
            "risk_debate_state": {
                "risky_history": final_state["risk_debate_state"]["risky_history"],
                "safe_history": final_state["risk_debate_state"]["safe_history"],
                "neutral_history": final_state["risk_debate_state"]["neutral_history"],
                "history": final_state["risk_debate_state"]["history"],
                "judge_decision": final_state["risk_debate_state"]["judge_decision"],
            },
            "investment_plan": final_state["investment_plan"],
            "final_trade_decision": final_state["final_trade_decision"],
        }

        # Save to file
        directory = Path(f"eval_results/{self.ticker}/TradingAgentsStrategy_logs/")
        directory.mkdir(parents=True, exist_ok=True)

        with open(
            f"eval_results/{self.ticker}/TradingAgentsStrategy_logs/full_states_log_{trade_date}.json",
            "w",
        ) as f:
            json.dump(self.log_states_dict, f, indent=4)

    def reflect_and_remember(self, returns_losses):
        """Reflect on decisions and update memory based on returns."""
        self.reflector.reflect_bull_researcher(
            self.curr_state, returns_losses, self.bull_memory
        )
        self.reflector.reflect_bear_researcher(
            self.curr_state, returns_losses, self.bear_memory
        )
        self.reflector.reflect_trader(
            self.curr_state, returns_losses, self.trader_memory
        )
        self.reflector.reflect_invest_judge(
            self.curr_state, returns_losses, self.invest_judge_memory
        )
        self.reflector.reflect_risk_manager(
            self.curr_state, returns_losses, self.risk_manager_memory
        )

    def process_signal(self, full_signal, structured_decision: dict | None = None):
        """Process a signal to extract the core decision."""
        return self.signal_processor.process_signal(full_signal, structured_decision)
