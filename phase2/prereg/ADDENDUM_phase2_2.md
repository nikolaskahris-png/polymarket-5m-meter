# ADDENDUM 2 στο PREREG_phase2 (16/11/2026, πριν από οποιαδήποτε μέτρηση)

Το πρώτο τρέξιμο του phase2-select σταμάτησε στο συνθετικό τεστ, πριν διαβαστεί οποιοδήποτε δεδομένο: το αρχείο `phase2/select.py` είχε το όνομα του module `select` της Python και το έκρυβε (circular import στο `subprocess`, Python 3.12 του runner).
Αλλαγή: μετονομασία σε `phase2/pick_targets.py` (αλλάζει μόνο η 1η γραμμή του docstring) και η αντίστοιχη γραμμή στο `.github/workflows/phase2-select.yml`. Καμία αλλαγή λογικής. Νέα sha256 στο SCRIPTS_2.sha256· το SCRIPTS.sha256 μένει όπως ήταν (append-only).
