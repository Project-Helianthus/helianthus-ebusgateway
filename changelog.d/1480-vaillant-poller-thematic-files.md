## Split semantic_vaillant.go into thematic files with a committed declaration-move script

The Vaillant semantic poller's declarations move, unchanged, from one 8,700-line file into thematic files (registers, discovery, zones, hot water, circuits, boiler, system, radio devices, FM5, energy, schedules, bus access, codec, publication) by the committed script `scripts/move_go_declarations.py` and its manifest. Behaviour and published output are unchanged; the gateway binary changes in its file and line tables only.
