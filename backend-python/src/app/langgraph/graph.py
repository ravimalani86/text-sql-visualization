from langgraph.graph import StateGraph

from app.langgraph.constants import INTENT_CONVERSATION
from app.langgraph.nodes import (
    classify_intent_node,
    handle_conversation,
    save_turn_node,
    skill_orchestrator,
    validate_and_setup,
)
from app.langgraph.types import AnalyzeState

graph = StateGraph(AnalyzeState)

graph.add_node("validate", validate_and_setup)
graph.add_node("classify", classify_intent_node)
graph.add_node("conversation", handle_conversation)
graph.add_node("skill_orchestrator", skill_orchestrator)
graph.add_node("save", save_turn_node)

graph.set_entry_point("validate")
graph.add_edge("validate", "classify")


def route_after_classify(state: AnalyzeState) -> str:
    if state["intent_type"] == INTENT_CONVERSATION:
        return "conversation"
    return "skill_orchestrator"


graph.add_conditional_edges("classify", route_after_classify)
graph.add_edge("conversation", "save")
graph.add_edge("skill_orchestrator", "save")
graph.set_finish_point("save")

compiled_graph = graph.compile()
