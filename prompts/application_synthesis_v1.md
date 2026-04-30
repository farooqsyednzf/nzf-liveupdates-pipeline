# Application Synthesis Prompt (v1)

You are writing a single sentence describing an applicant to NZF Australia. The output appears on the public NZF website to convey unmet need with dignity. Read the case context below and either produce ONE synthesised sentence or output the literal string `SKIP`.

## What to write

One sentence, between 60 and 90 characters, covering:

- WHO: relational/role term only (brother, sister, single mother, widow, father, family, student, elderly gentleman). Never real names.
- WHAT: specific concrete need (housing, rent, food, medical bills, tuition, utilities). Avoid vague phrases like "financial support" alone.
- WHY: actual circumstance where possible (fleeing domestic violence, redundancy, eviction, illness, NDIS waiting, visa restrictions, etc.).

## Voice

- Third-person summary. NEVER first-person ("I am..." / "I need...").
- Warm and respectful. Recipients have a right to Zakat. No pity language.
- Australian English spelling.
- No em dashes or en dashes anywhere.

## Examples of good output

- A father caring for his son with a disability was evicted after six years and urgently needs housing.
- A single mother of five studying while on Centrelink is struggling to cover bills and household expenses.
- A young sister who recently converted to Islam was cut off by her family and urgently needs accommodation.
- A brother recently made homeless after losing his job urgently needs shelter, food, and transportation.
- A mother of two children with disability and an unwell husband cannot afford school books or food.

## When to SKIP

Output the literal string `SKIP` (uppercase, nothing else) if any of these apply:

- The case context is incoherent or untranslatable (e.g. Arabic-only with no translation).
- The request is for a loan.
- The request is for funeral or burial assistance.
- The only available text is a generic opener ("I hope this message finds you well", "I am writing to request financial assistance") with no specific circumstance, family detail, or hardship described anywhere.
- The text is clearly an email forward or staff comment, not the client's circumstance (e.g. "Dear NZF Case Worker", "Caution: This email originated from outside the organisation").

## Sensitive content

If the case references suicide attempts or self-harm, focus on the underlying hardship (debt, exhaustion, isolation) without referencing self-harm.

## Output format

Either ONE sentence (60 to 90 characters) OR the literal string `SKIP`. No quotes, no preamble, no explanation, no labels.

## Case context

{context}

## Your output
