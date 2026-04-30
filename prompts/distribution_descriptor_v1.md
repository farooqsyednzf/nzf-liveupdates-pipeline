# Distribution Descriptor Prompt (v1)

You are generating a single short empathetic descriptor for an NZF Australia Zakat distribution. Read the case context below and choose ONE descriptor from the priority hierarchy.

## Priority Hierarchy

Apply the FIRST matching category in order:

1. Domestic violence / family violence -> "a single mother fleeing domestic violence" or "a family escaping domestic violence"
2. Widowed with children -> "a widowed mother with young children"
3. Single mother caring for a child with special needs -> "a single mother caring for a child with special needs"
4. Single mother in emergency shelter -> "a single mother in emergency shelter"
5. Single mother with newborn -> "a single mother with a newborn"
6. Single mother (general) -> "a single mother in financial hardship"
7. Homelessness -> "a family facing homelessness"
8. Heart surgery / serious surgery -> "a brother recovering from heart surgery"
9. Cancer / chronic illness -> "a family affected by chronic illness"
10. Disability / NDIS -> "a family with a member with disability"
11. Refugee / asylum seeker with children -> "a refugee family with young children"
12. Student family, pregnant -> "a pregnant sister with no income"
13. Student family (general) -> "a student family facing financial hardship"
14. Gaza displaced -> "a Gaza-displaced family rebuilding in Australia"
15. Eviction / rent arrears -> "a family facing eviction due to rent arrears"
16. Pensioner -> "an elderly pensioner in financial hardship"
17. Health / ill / medical (general) -> "a brother facing health challenges and unemployment"
18. Generic family hardship -> "a family in financial hardship"

## Rules

- Output ONLY the descriptor text. No quotes, no preamble, no explanation.
- Use relational terms: brother, sister, single mother, family, etc. Never use real names.
- If the case mentions a suicide attempt or self-harm, focus on the underlying hardship (debt, exhaustion, isolation) and pick descriptor 18 ("a family in financial hardship") or another non-clinical option from the list. Never reference self-harm.
- If unclear, default to descriptor 18.

## Case context

{context}

## Your output (descriptor only)
