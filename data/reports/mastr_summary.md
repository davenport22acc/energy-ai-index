# MaStR DE, Stand 2026-11-19

Einheiten Wind, PV und Speicher ab 1 MW. Quelle: Marktstammdatenregister, Datenlizenz Deutschland 2.0.

## Nach Technologie und Status

| technology   | status                    |   units |    mw |
|:-------------|:--------------------------|--------:|------:|
| solar        | In Betrieb                |    8446 | 39614 |
| solar        | In Planung                |     790 |  7428 |
| solar        | Endgültig stillgelegt     |      20 |    84 |
| solar        | Vorübergehend stillgelegt |       6 |     9 |
| storage      | In Betrieb                |     680 | 14498 |
| storage      | In Planung                |     737 | 12987 |
| storage      | Endgültig stillgelegt     |      11 |   134 |
| storage      | Vorübergehend stillgelegt |      10 |    17 |
| wind         | In Betrieb                |   26634 | 80969 |
| wind         | In Planung                |    8696 | 55249 |
| wind         | Endgültig stillgelegt     |    1919 |  3122 |
| wind         | Vorübergehend stillgelegt |      18 |    29 |

## Pipeline (nicht in Betrieb) nach Bundesland, MW

| state                           |   solar |   storage |   wind |
|:--------------------------------|--------:|----------:|-------:|
| Nordrhein-Westfalen             |     380 |      1559 |  13722 |
| Niedersachsen                   |     603 |       561 |  12540 |
| Brandenburg                     |    1245 |      2594 |   6109 |
| Bayern                          |    2031 |      1876 |   2810 |
| Mecklenburg-Vorpommern          |     994 |       716 |   3689 |
| Schleswig-Holstein              |     369 |       500 |   3766 |
| Sachsen                         |     153 |      2969 |   1159 |
| Sachsen-Anhalt                  |     595 |       638 |   3014 |
| Rheinland-Pfalz                 |     272 |       592 |   3319 |
| Hessen                          |     179 |       137 |   2993 |
| Baden-Württemberg               |     369 |       706 |   1921 |
| Thüringen                       |     255 |       175 |   1665 |
| Ausschließliche Wirtschaftszone |       0 |         0 |   1320 |
| Saarland                        |      46 |        35 |    329 |
| Bremen                          |       0 |        65 |     32 |
| Berlin                          |      23 |        10 |      0 |
| Hamburg                         |       8 |         5 |     12 |

## Gefundene Spalten

- wind: unit_id=EinheitMastrNummer, name=NameStromerzeugungseinheit, status=EinheitBetriebsstatus, kw=Nettonennleistung, state=Bundesland, plz=Postleitzahl, municipality=Gemeinde, lat=Breitengrad, lon=Laengengrad, operator_id=AnlagenbetreiberMastrNummer, commissioning=Inbetriebnahmedatum, planned_commissioning=GeplantesInbetriebnahmedatum, registered=Registrierungsdatum, permit_id=GenMastrNummer, eeg_id=EegMastrNummer, location_id=LokationMastrNummer
- solar: unit_id=EinheitMastrNummer, name=NameStromerzeugungseinheit, status=EinheitBetriebsstatus, kw=Nettonennleistung, state=Bundesland, plz=Postleitzahl, municipality=Gemeinde, lat=Breitengrad, lon=Laengengrad, operator_id=AnlagenbetreiberMastrNummer, commissioning=Inbetriebnahmedatum, planned_commissioning=GeplantesInbetriebnahmedatum, registered=Registrierungsdatum, permit_id=GenMastrNummer, eeg_id=EegMastrNummer, location_id=LokationMastrNummer
- storage: unit_id=EinheitMastrNummer, name=NameStromerzeugungseinheit, status=EinheitBetriebsstatus, kw=Nettonennleistung, state=Bundesland, plz=Postleitzahl, municipality=Gemeinde, lat=Breitengrad, lon=Laengengrad, operator_id=AnlagenbetreiberMastrNummer, commissioning=Inbetriebnahmedatum, planned_commissioning=GeplantesInbetriebnahmedatum, registered=Registrierungsdatum, permit_id=GenMastrNummer, eeg_id=EegMastrNummer, location_id=LokationMastrNummer
