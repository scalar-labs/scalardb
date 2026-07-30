# Collation — things to verify / open questions

A running list of items to confirm or resolve for the `scalar.db.collation` feature. Each item
states the risk, how to verify it, and the candidate fix. Check items off as they are resolved.

---

## V1. ICU locale string is parsed as a legacy ICU locale ID, not BCP-47 — collation keywords may be silently dropped

- **Status:** ☐ to verify
- **Area:** shipped ordering-only feature — `core/src/main/java/com/scalar/db/io/CollationComparator.java` (`buildValidatedLocaleCollator`).
- **Risk:** silent wrong ordering the operator cannot detect (same failure class as an unknown
  locale, but one level up: a *dropped collation keyword* rather than an unknown language).

**What happens.** The collator is built with `Collator.getInstance(new ULocale(localeName))`.
`new ULocale(String)` parses the **legacy ICU locale ID** format, which does **not** interpret a
BCP-47 `-u-` Unicode extension. So a modern BCP-47 tag loses its collation keyword:

| Configured `scalar.db.collation.icu.locale` | What ICU4J is expected to use |
|---|---|
| `ja-u-co-unihan` (BCP-47) | `ja` standard — `-u-co-unihan` dropped |
| `ja-u-kn-true` (BCP-47 numeric) | `ja` standard — extension dropped |
| `ja@collation=unihan` (legacy ICU ID) | `unihan` ✓ |
| `ja@colnumeric=yes` (legacy ICU ID) | numeric ✓ |

**Why the validation misses it.** `buildValidatedLocaleCollator` rejects a locale only when
`VALID_LOCALE` resolves empty (unknown language). `ja-u-co-unihan` still resolves to a valid `ja`,
so it passes validation and silently returns the *standard* `ja` collation instead of the requested
tailoring.

**How to verify (against the bundled ICU4J 77.1):**
1. Does `new ULocale("ja-u-co-unihan")` actually drop the extension on 77.1? Compare
   `new ULocale("ja-u-co-unihan")` vs `ULocale.forLanguageTag("ja-u-co-unihan")` — check
   `.getKeywordValue("collation")` and the resulting collator's ordering on a discriminating pair.
2. Which collation tailorings actually ship for a locale in our `icu4j.jar`:
   `Collator.getKeywordValuesForLocale("collation", new ULocale("ja"), false)` (enumerates the
   real collation types, e.g. whether `unihan` is present).
3. Confirm `kn` (numeric) ordering is a runtime attribute (expected always-available) vs a
   data-shipped tailoring.

**Candidate fixes (pick one, then tighten validation either way):**
- **Option A — accept BCP-47:** switch to `ULocale.forLanguageTag(localeName)` so `-u-` extensions
  work, and document the BCP-47 form in `docs/collation.md`.
- **Option B — keep legacy IDs:** keep `new ULocale()` and document the legacy `@collation=` /
  `@colnumeric=` syntax explicitly (so operators don't assume BCP-47 works).
- **Validation (both options):** compare the requested collation keyword against
  `Collator.getKeywordValuesForLocale(...)` and **reject an unsupported keyword** at startup, rather
  than silently degrading to the standard collation.

**Relationship to other items:** same "silent-wrong-ordering the operator can't detect" shape as
the unrecognized-locale guard already added (`CORE-0297`) and the ICU-version-drift caveat in
`docs/collation-storage-compatibility.md`. It also affects the planned collation-aware **equality**
feature, since that reuses the same configured collator.
