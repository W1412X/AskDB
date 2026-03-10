"""
Durable dialog repository backed by GlobalState.dialog_state.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from stages.sql_generation.dag.models import DialogTicketRecord, GlobalState
from stages.sql_generation.intent.models import DialogResolutionType
from utils.id_generator import new_id

class DialogRepository:
    def __init__(self, state: GlobalState) -> None:
        self._state = state

    def create_ticket(
        self,
        *,
        intent_id: str,
        question_id: str,
        phase: str,
        payload: Dict[str, Any],
        thread_id: Optional[str] = None,
    ) -> DialogTicketRecord:
        ticket_id = new_id("dlg")
        record = DialogTicketRecord(
            ticket_id=ticket_id,
            intent_id=str(intent_id),
            question_id=str(question_id),
            phase=str(phase),
            created_at=time.time(),
            payload=dict(payload),
            thread_id=thread_id or new_id("thread"),
        )
        self._state.dialog_state.tickets[ticket_id] = record
        self._state.dialog_state.queue.append(ticket_id)
        return record

    def get_active_ticket(self) -> Optional[DialogTicketRecord]:
        active_id = self._state.dialog_state.active_ticket_id
        if active_id:
            return self._state.dialog_state.tickets.get(active_id)
        while self._state.dialog_state.queue:
            ticket_id = self._state.dialog_state.queue.pop(0)
            ticket = self._state.dialog_state.tickets.get(ticket_id)
            if ticket and not ticket.resolved:
                self._state.dialog_state.active_ticket_id = ticket_id
                return ticket
        return None

    def append_turn(self, *, ticket_id: str, user_message: str, parsed: Optional[Dict[str, Any]] = None) -> DialogTicketRecord:
        ticket = self._state.dialog_state.tickets.get(ticket_id)
        if ticket is None:
            raise ValueError(f"unknown ticket_id: {ticket_id}")
        ticket.turns.append({"user_message": str(user_message), "parsed": parsed or {}, "created_at": time.time()})
        return ticket

    def mark_resolved(self, ticket_id: str, resolution_type: DialogResolutionType) -> DialogTicketRecord:
        ticket = self._state.dialog_state.tickets.get(ticket_id)
        if ticket is None:
            raise ValueError(f"unknown ticket_id: {ticket_id}")
        ticket.resolved = True
        ticket.resolution_type = resolution_type
        if self._state.dialog_state.active_ticket_id == ticket_id:
            self._state.dialog_state.active_ticket_id = None
        return ticket

    def get_ticket(self, ticket_id: str) -> Optional[DialogTicketRecord]:
        return self._state.dialog_state.tickets.get(ticket_id)

    def list_pending_tickets(self) -> List[DialogTicketRecord]:
        return [
            ticket
            for ticket in self._state.dialog_state.tickets.values()
            if ticket and not ticket.resolved
        ]


def get_dialog_repository(state: GlobalState) -> DialogRepository:
    return DialogRepository(state)
