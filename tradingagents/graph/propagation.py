# TradingAgents/graph/propagation.py

from typing import Dict, Any
from tradingagents.agents.utils.agent_states import (
    AgentState,
    InvestDebateState,
    RiskDebateState,
)
from tradingagents.market_utils import build_security_profile


class Propagator:
    """Handles state initialization and propagation through the graph."""

    def __init__(self, max_recur_limit=100, market_region="cn_a"):
        """Initialize with configuration parameters."""
        self.max_recur_limit = max_recur_limit
        self.market_region = market_region

    def create_initial_state(
        self,
        company_name: str,
        trade_date: str,
        source_context: Dict[str, Any] | None = None,
        overnight_context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Create the initial state for the agent graph."""
        security_profile = build_security_profile(company_name, self.market_region)
        return {
            "messages": [("human", company_name)],
            "company_of_interest": security_profile.normalized_ticker,
            "trade_date": str(trade_date),
            "market_region": security_profile.market_region,
            "security_profile": security_profile.to_dict(),
            "investment_debate_state": InvestDebateState(
                {
                    "history": "",
                    "bull_history": "",
                    "bear_history": "",
                    "latest_speaker": "",
                    "current_response": "",
                    "judge_decision": "",
                    "count": 0,
                }
            ),
            "risk_debate_state": RiskDebateState(
                {
                    "history": "",
                    "current_risky_response": "",
                    "current_safe_response": "",
                    "current_neutral_response": "",
                    "count": 0,
                }
            ),
            "market_report": "",
            "fundamentals_report": "",
            "sentiment_report": "",
            "news_report": "",
            "source_context": source_context or {},
            "overnight_context": overnight_context or {},
            "factor_snapshot": {},
            "evidence_snapshot": {},
            "structured_decision": {},
        }

    def get_graph_args(self) -> Dict[str, Any]:
        """Get arguments for the graph invocation."""
        return {
            "stream_mode": "values",
            "config": {"recursion_limit": self.max_recur_limit},
        }
