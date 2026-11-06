# Instrukce pro agenty

Zadání, pravidla a denní plán tohoto hackathonu jsou v Google dokumentu. Před jakoukoli implementací ho přečti a řiď se jím. Když se tento soubor a dokument rozcházejí, platí dokument.

**Zdroj pravdy:** [AI Hackaton](https://docs.google.com/document/d/1yCSgx7ab3q2mFctdjXTIlBBMyS0JJASxGkGwyvxaB2U/edit?tab=t.0)

V dokumentu je brief AT&T Hackathon (Efficient AI Context Service for Enterprise Engineering Knowledge), hodnoticí kritéria, typy zdrojů, záměrně nedokonalá data a pracovní plán 9:30–16:00. Sekce 3 (Implementation Hint) a 4 (Target Deliverable for the PoC) v briefu chybí — na fotkách zadání nebyly.

## Co z dokumentu plyne

- Stavíme automatizovaný Context Service. Pipeline sama načte zdroje, roztřídí je, uloží fakta a najde rozpory. Znalosti se nevypisují ručně.
- Jediný zdroj dat je repozitář [arkik7/att-hackathon-task](https://github.com/arkik7/att-hackathon-task). Jen ho čteme. Nic v něm neupravujeme a nic do něj necommitujeme.
- Každá odpověď, kterou systém vrátí, má tvrzení, zdroj a míru jistoty. Fakt bez zdroje se do odpovědi nedostane.
- Systém nespojuje všechno. Stejné API ve dvou projektech ještě není vztah, dokud na to není evidence. Rozlišuje ověřená fakta, předpoklady, zastaralé informace, rozpory, slabou evidenci a bezpečnostní rizika.
- Všechny volání modelů jdou přes OpenRouter. Klíč není v tomto repozitáři — bere se z prostředí, ne z dokumentu a ne z gitu.
- Dnešní řez je malý PoC: pipeline nad tím repozitářem sama odpoví aspoň na tři otázky (souvislosti, rozpory, bezpečnostní riziko). Demo i vysvětlení mají být hotové v 16:00. Vysvětlení návrhu, trade-offů, limitů a dalšího kroku má v hodnocení největší váhu.
