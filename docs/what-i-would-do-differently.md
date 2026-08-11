# What I Would Do Differently

This section is updated as the project progresses — not written retroactively at the end. Honest engineering retrospectives, including things that didn't work on the first attempt.

## Confirm the execution environment before designing around it

I designed the silver/gold notebooks for Microsoft Fabric Lakehouse from the start, without first confirming I'd have reliable, dedicated access to a Fabric environment. I'd only discover the licensing/capacity obstacles (shared trial contention, Developer Program rejections, tenant conflicts) after the design was already built around Fabric-specific assumptions. Next time, I'd validate the target execution environment is actually accessible — even with a throwaway "hello world" job — before writing real pipeline code against it.

## Test the ingestion connector with a tiny payload before building the full pipeline around it

I built out the entire parameterized ADF pipeline (5 cities, dynamic date ranges) on top of the REST connector before discovering it had a real bug decoding Open-Meteo's response — a bug that would have shown up immediately with a single manual test call. A 30-second smoke test against one city, one week of data, would have caught the REST-vs-HTTP+Binary issue before I'd built anything on top of it.

## Review generated business logic critically, not just for "does it run"

The gold-layer anomaly calculation ran without errors on the first try — but it had a real lookahead-bias bug (comparing a year against a baseline that included future years). It only surfaced because of a deliberate second pass asking "does this logic make sense," not because anything crashed. I'd build in that critical-review step earlier and more routinely, rather than treating "it executed successfully" as equivalent to "it's correct."

## Don't let platform friction consume the whole session

A significant part of one working session went into Microsoft account/licensing troubleshooting (Fabric trial capacity, Developer Program sign-up, tenant conflicts) that had nothing to do with data engineering skill. In hindsight, I'd set a firmer time limit on that kind of friction — say, 30 minutes — before pivoting to a workaround (local execution), rather than continuing to retry variations of the same blocked path.

## Build real understanding of generated code, not just working code

Claude Code wrote correct, well-structured PySpark for the silver/gold notebooks — but I could read and follow it without being able to independently explain or defend the `arrays_zip`/`explode` logic under interview-style questioning. Next project, I'd build in a deliberate "explain this back in my own words" checkpoint right after each major piece of generated logic, instead of treating "the code works and I read it" as equivalent to "I understand it well enough to defend it."
