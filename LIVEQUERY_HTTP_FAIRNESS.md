# LiveQuery / blocco HTTP — 26 ottobre 2026

## Esito e provenienza

Correzione nei sorgenti del fork `Messagenius/parse-server`, branch locale `codex/livequery-http-fairness`, worktree `/Users/gkk/Projects/spikad/worktrees/livequery-http-fairness/parse-server`, a partire da `d0fbc892b2944de1e3d1706e3bb53e583325d0ea` (Parse 9.10.0-messagenius.1). Il checkout di riferimento è stato trovato in `/private/tmp/spikad-review-20261026-nS2Qa5/sources/parse-server`; la dipendenza in `worktrees/2.5.13-opt9/base-api/package.json` certifica lo stesso commit. Il base-api principale usa Parse 7 e non è stato modificato. Nessun server, configurazione di produzione, pin di release, commit remoto o deploy modificato.

Gli hash dei due file nella release locale coincidono con quelli forniti per produzione:

- `ParseLiveQueryServer.js`: `20ccc7dbfeb94e19800aa6e42c21a523aecdc1075aa954dbbb95b8ca9c9e40dd`.
- `FanoutLimiter.js`: `8d9bade48d757fdd1812a88bd3d6dd0e990c94478fb0c82a3b8dd800e3da2288`.

Questo verifica il riferimento su disco, non quanto era caricato nei 12 worker. Le nuove metriche includono PID, istanza, timestamp e `implementation: cooperative-fanout-v1` per riconoscere il percorso realmente in esecuzione.

## Causa dimostrata e limiti dell'attribuzione

La build precedente riproduce starvation dell'event loop nel fanout: anche con concorrenza limitata, enumerazione sincrona e catene di Promise tengono occupato il ciclo senza servire HTTP e timer. Il generatore indipendente continua a inviare eventi quando il worker è bloccato; il test non si autoregola rallentando insieme al server. Il percorso corretto conserva la reattività anche quando il lavoro in ingresso supera la capacità e la coda cresce. Le regressioni isolate coprono separatamente drain di Promise ed enumerazione di destinatari esclusi/query non corrispondenti.

È una causa dimostrata nella riproduzione e coerente con CPU, timeout localhost e riepilogo del profilo di produzione; non è la prova che spieghi da sola l'intero incidente del 26 ottobre. I SupportReport `b40ABudIJ6` e `T1lJ2gNAnV` non risultano scaricati in `logs/profiling`; le due attachment contengono riepiloghi, non profili grezzi o metadata completi. Il downloader esistente richiede URL e master key interattiva, non disponibili in questa sessione. Non sono stati sostituiti con report di altri giorni.

Il collector locale calcola CPU SELF da hitCount, non da timeDeltas; le allocazioni sommano selfSize includendo oggetti già raccolti. I valori etichettati MB nel riepilogo sono MiB. La sovrapposizione 3879 ms è calcolata dall'orologio diagnostico fra ACK di avvio e richiesta stop. Non attribuiamo tutto il 60% a JSON cloning, non deduciamo leak, tasso esatto di allocazione o causa esclusiva dalla RAM. Redis, Mongo e la discrepanza PM2 daemon/CLI restano privi di prova causale diretta.

## Modifiche

- [FanoutLimiter](src/LiveQuery/FanoutLimiter.js): budget condiviso di 64 passi fra enumerazione e avvii; solo `setImmediate` ripristina il budget. I completamenti Promise non lo ricaricano. Concorrenza massima resta 256, configurabile con `MSG_LQ_FANOUT_CONCURRENCY` come prima. I producer degli eventi sono FIFO; il percorso ordinario materializza al massimo una callback in attesa oltre quelle attive. Gli eventi restano accodati senza scadenze, coalescing o eliminazione per pressione.
- [FanoutRecipients](src/LiveQuery/FanoutRecipients.js) e [Subscription](src/LiveQuery/Subscription.js): indice incrementale e cutoff alla ricezione dell'evento. Esclude registrazioni successive, evita copie sincrone dell'intero elenco e gestisce unsubscribe/replacement durante una cessione. Token di validità separati dai nodi; cursori aggiornati all'unlink per non trattenere catene di registrazioni rimosse.
- [ParseLiveQueryServer](src/LiveQuery/ParseLiveQueryServer.ts): save e delete enumerano cooperativamente anche quando le ACL escludono tutti o nessuna query corrisponde. ACL lette una volta per versione dell'evento; CLP usa lo snapshot JSON già disponibile; watch usa gli attributi SDK acquisiti per evento. Nessuna decisione di autorizzazione viene riutilizzata fra eventi o utenti.
- Serializzazione JSON lazy una volta per versione dell'evento; parsing in una copia privata soltanto dopo CLP, ACL e watch. Su leave per revoca ACL si copia solo lo stato precedentemente autorizzato; su enter per concessione ACL si omette l'originale non autorizzato. Trigger e protected fields continuano a ricevere oggetti isolati.
- Identità client/subscription ricontrollata a dequeue e dopo auth, trigger e filtraggio: un requestId riutilizzato non riceve il vecchio evento, neppure un errore tardivo. Subscribe ricontrolla inoltre che il client esista dopo il lavoro asincrono, prima della registrazione, evitando subscription zombie dopo disconnect.

Ordine: rimane FIFO l'ammissione di save/delete e il lavoro per ciascuna registrazione. La build precedente già consentiva il completamento fuori ordine di callback concorrenti con attese diverse; questa patch non promette ordinamento globale delle consegne. L'ordine relativo fra client differenti segue ora la registrazione anziché il raggruppamento per client. Non cambia il contratto di ordinamento per un destinatario. Una query, serializzazione o trigger sincrono individualmente costoso non è interrompibile dal budget.

## Misure riproducibili

Vedere [metodo, tabella completa e allocazioni](spec/regression/http-fairness.bench.md), [harness](spec/regression/http-fairness.bench.cjs) e [19 risultati JSON](spec/regression/benchmark-results/). Node 22.14.0/macOS, non Node 20.20.0/Linux di produzione; 4000 subscriber, metà esclusi da ACL a ruoli, cache auth calda, prefilter sempre non applicabile. HTTP reale su loopback, generatori in worker thread indipendente, serializzazione WebSocket reale con trasporto simulato. Nessun Redis, Mongo o 4000 socket reali; trigger di produzione esclusi da questa misura e verificati semanticamente nei test.

| Carico | HTTP p95 prima → dopo | Timer p95 prima → dopo | Candidati/s prima → dopo | CPU prima → dopo |
|---|---:|---:|---:|---:|
| 8 eventi/s | 59,26 → 0,89 ms | 55,59 → 1,29 ms | 32.000 → 32.000 | 65,7 → 28,1% |
| 25 eventi/s | 8.257,53 → 0,95 ms | 8.529,54 → 1,27 ms | 70.000 → 99.900 | 112,6 → 74,8% |
| 75 eventi/s, solo dopo | 0,89 ms | 0,18 ms | 133.000 su 300.000 offerti | 95,8% |

A 25 eventi/s il massimo HTTP passa da 8.705,51 a 8,31 ms; backlog massimo 189.110 → 13.110 candidati equivalenti; crescita mediana 28.600/s → circa zero; smaltimento mediano dopo stop 2.566 → 3 ms. A 75 eventi/s il backlog arriva a 1.034.563, cresce di circa 171.600/s e richiede 7,53 s mediani per smaltirsi. Tutte le consegne attese completate, zero errori o frame a utenti non autorizzati.

Sono mediane di tre processi separati; allocazioni in prove inspector separate con intervallo effettivo comprensivo dello smaltimento. Throughput di notifica è metà del throughput candidato perché metà dei destinatari è esclusa. CPU include worker generatore e probe HTTP, può superare il 100%. Il massimo della coda end-to-end include ingresso non ancora consumato: non va confrontato direttamente con `fanout.queued` o `queuedEvents` che hanno unità diverse.

Allocazioni campionate per 600.000 candidati: 11,566 → 8,389 GB (−27,5%), con durata effettiva del campionamento 9,290 → 6,007 s. Non confrontare solo GB/s: la versione nuova esaurisce prima il lavoro. A 8 eventi/s la riduzione è 27,7%. Heap post-GC nei trial senza profiler: 42,30–42,90 MB prima contro 48,12–48,15 MB dopo. Non c'è una riduzione dimostrata della memoria trattenuta: l'indice aggiunto e il diverso stato warmup/JIT hanno un costo. Sei secondi per trial non dimostrano né escludono un leak; i test di churn verificano specificamente che registrazioni rimosse non restino collegate.

La prova a 75 eventi/s dimostra equità con HTTP, non capacità illimitata: la coda cresce e richiede diversi secondi per smaltirsi. Non c'è limite di memoria della coda eventi. Occorre ridurre lavoro/ingresso o introdurre separatamente una politica esplicita di admission/replay; nessuna perdita silenziosa è stata introdotta.

## Osservabilità

Una riga `LiveQuery metrics` ogni 10 secondi per istanza e PID, sul logger Parse a livello info. `MSG_LQ_METRICS_INTERVAL_MS` permette un intervallo intero >=1000 ms; 0 disattiva. Il logger deve consentire info. Timer unref e cleanup in shutdown; nessuna route HTTP né callback generico necessaria. Se un altro codice blocca completamente il thread anche questi log vengono ritardati: usare un probe esterno per rilevare l'assenza di campioni, non assumere valori freschi.

`clients` conta i client registrati; `clientSubscriptions` le registrazioni effettive; `classes` le classi; `distinctQueries` le definizioni deduplicate tramite queryHash. Eventi/candidati/notifiche/scarti e motivi sono cumulativi per istanza; fare differenze fra campioni dello stesso PID/istanza. Errori sono separati dagli scarti. Le notifiche sono invii al trasporto, non ACK del client. `fanout` espone attivi, callback in coda, eventi da enumerare, picchi, età più vecchia, yield ed errori. Il ritardo event loop riporta media/p99/max nella finestra. Nessun token, identificatore utente, query o payload nelle nuove metriche. I log legacy non sono stati sanificati globalmente.

## Test di correttezza

**66/66 regressioni node:test passate**, compilazione Babel e controllo TypeScript riusciti, ESLint dei file JS/test interessati e `git diff --check` puliti. Il file TS è controllato da tsc e formattato con Prettier; la configurazione ESLint del repository non lo include. Il test subscribe/disconnect falliva sulla build precedente con una registrazione residua e passa con la guardia. Anche le consegne obsolete dopo attese auth/trigger/filter sono state riprodotte prima della correzione.

Le 19 misure comprendono il fix dei token e cursori; la guardia finale subscribe/disconnect non è nel percorso del benchmark (che registra i client in memoria). Una prova aggiuntiva della build finale a 25 eventi/s è archiviata in `spec/regression/benchmark-results/final-build-smoke.json`. Il manifest `build-verification.json` nella stessa directory registra hash dei sorgenti e lib candidate; non è una versione pubblicata.

Copertura: ACL utenti/ruoli e token distinti; CLP pointer/readUserFields/requiresAuthentication; protected fields; trigger concorrenti con mutazioni nested; enter/leave con revoca/concessione ACL; watch; save/delete FIFO; flusso continuo con autorizzazioni negate e query non corrispondenti; disconnect/unsubscribe/requestId reuse prima e durante auth/trigger/filter; eccezioni e avanzamento; registrazioni successive al cutoff; churn dei registri durante callback/producer sospesi; conteggi metriche dopo sostituzioni ripetute e cleanup timer. La fixture Jasmine che scriveva direttamente nella mappa è stata allineata all'API di Subscription.

Non eseguita la suite completa di integrazione Mongo/Redis del progetto. I test e il benchmark non certificano comportamento di rete, backpressure WebSocket, carico di 12 worker, durata ore o Node 20 di produzione. Questi rimangono criteri della prova sul singolo nodo.

## Seconda fase: amplificatori verificati e verifiche residue

1. **Replicazione per worker/istanza.** `worktrees/2.5.13-opt9/base-api/index.js:683–720` crea una LiveQuery per listener e, con porta interna distinta e `MSG_LQ_SINGLE_SERVER` falso, una seconda. Ogni istanza sottoscrive i canali applicativi e parse/inflate precede la selezione delle subscription. Contare le istanze nei nuovi log e i client sul listener interno prima di cambiare topologia. L'adapter ioredis locale evita la doppia callback locale: replicazione fra istanze non equivale a bug Redis.
2. **Query ampie e ACL negate.** Il rapporto candidati/notifiche/scarti quantifica il costo; misurare classi/query aggregate senza loggare contenuti. L'ottimizzazione evita copie inutili ma non elimina il costo di match/autorizzazione.
3. **Trigger.** La release fa fetch per destinatario in Conversation (Avatar), BlastLite (fino a tre query), Friendship (fino a due) e Story (media). Presenza dipende da `MSG_PRESENCE_FROM_LQ_EVENTS` e dai relativi intervalli. Il trigger MessageInfo scrivente importato viene sovrascritto dal successivo handler di solo logging in `main.js:607`: non usarlo come prova di un loop attivo. Misurare durata/count query e scritture per evento prima di attribuire loop.
4. **Socket e riconnessioni.** `ParseWebSocket.send` chiama ws.send senza attendere flush o controllare bufferedAmount. Fanout drenato non implica socket drenati. Rilevare somma/max dei buffer, errori e ritmo connect/disconnect. Ping/pong ritardati dalla starvation possono amplificare riconnessioni; non è provato nei report disponibili. ESTAB/CLOSE-WAIT non sono numero client LQ.
5. **HTTP/middleware.** La release monta `/` che risponde OK, mentre `/MSG-health` attende una scrittura HealthCheck. CORS/Helmet/header/rate-limit opzionale/access-log/audit precedono le route; access log può serializzare body e accumulare response chunk. Timeout anche su `/` è compatibile con starvation o middleware comune, non dimostra lentezza della sola scrittura health. Il mount `/health` versus `/v3/health` dipende dalla configurazione; non sono stati modificati.
6. **Parse interno.** `directAccess: true` in base-api installa ParseServerRESTController: le normali query dei trigger passano dal router in-process, fallback HTTP solo quando non instradabili. Non attribuire autochiamate bloccate a localhost senza catturare quel fallback.
7. **Cluster.** Ecosystem locale (instances 0/restart 1G) diverge dai 12 worker/3G osservati. Verificare la configurazione effettivamente caricata e distribuzione client/eventi per PID. La discrepanza PM2 merita riallineamento separato e controllato, non è una spiegazione dimostrata né una modifica inclusa.

## Prova su un nodo e rollback (non eseguiti)

Prima integrare/revisionare il branch del fork e assegnare alla candidata un nuovo commit/versione; il package.json qui conserva la versione di partenza e non è una release pubblicata. Aggiornare il pin Parse nel base-api del ramo Parse 9, poi rigenerare il lock consumer e creare la release completa con la procedura esistente. Non puntare al base-api principale Parse 7 né modificare node_modules a mano. Conservare la release completa precedente, lock, configurazione e manifest hash; nessuna migrazione DB è richiesta da questa patch.

Togliere dal bilanciatore un solo nodo scelto. Installare la candidata completa con la procedura di deploy già collaudata, mantenendo heap, numero worker e configurazione invariati. Riavviare con il comando sotto. Verificare PID nuovi, hash del manifest candidato e almeno due campioni recenti `cooperative-fanout-v1` da ogni istanza prevista, prima di riportare una quota controllata di traffico sul nodo. La sola versione npm o gli hash su disco non verificano il codice caricato.

Misurare per almeno 15–30 minuti sotto carico rappresentativo e durante un burst: HTTP semplice p95/p99/errori, event-loop p99/max, crescita/età della coda eventi, candidati/notifiche/scarti/errori, CPU/RSS e buffer socket. Fermare l'espansione se coda/età crescono continuamente, HTTP peggiora rispetto ai peer, compaiono errori producer, anomalie di autorizzazione o crescita memoria persistente. Dopo riduzione del carico, esigere smaltimento della coda e ritorno delle latenze; un canary breve non è una prova di capacità a lungo termine.

Per rollback escludere nuovamente lo stesso nodo, ripristinare integralmente release precedente e relativa configurazione/lock con il meccanismo di deploy esistente, poi riavviare con lo stesso comando. Verificare tutti i PID e gli hash attesi della vecchia release. Il riavvio perde qualsiasi backlog in memoria come già accadeva prima: pianificare drain o resync dei client; la patch non introduce persistenza/replay. Nessun rollback di schema richiesto.

Tutti i comandi operativi sono nello stesso blocco. Le operazioni di installazione/ripristino della release restano quelle del sistema di deploy effettivo; non si presume un layout di symlink non verificato.

```sh
# Locale: verifiche del checkout candidato con un Node supportato nel PATH
npm run test:messagenius --prefix /Users/gkk/Projects/spikad/worktrees/livequery-http-fairness/parse-server
npm run build:types --prefix /Users/gkk/Projects/spikad/worktrees/livequery-http-fairness/parse-server
# Sul SOLO nodo canary, già escluso dal bilanciatore e dopo installazione della release candidata
node -p "require('/opt/msg/api-server/node_modules/parse-server/package.json').version"
sha256sum /opt/msg/api-server/node_modules/parse-server/lib/LiveQuery/ParseLiveQueryServer.js /opt/msg/api-server/node_modules/parse-server/lib/LiveQuery/FanoutLimiter.js /opt/msg/api-server/node_modules/parse-server/lib/LiveQuery/FanoutRecipients.js /opt/msg/api-server/node_modules/parse-server/lib/LiveQuery/Subscription.js
npm start --prefix /opt/msg/api-server
curl --connect-timeout 1 --max-time 4 --silent --show-error --output /dev/null --write-out 'root code=%{http_code} connect=%{time_connect} total=%{time_total}\n' http://127.0.0.1:10001/
tail -n 1000 /mnt/msg-logs/msg/out.log | grep 'LiveQuery metrics'
# Rollback: SOLO dopo esclusione del nodo e ripristino della release completa precedente
npm start --prefix /opt/msg/api-server
sha256sum /opt/msg/api-server/node_modules/parse-server/lib/LiveQuery/ParseLiveQueryServer.js /opt/msg/api-server/node_modules/parse-server/lib/LiveQuery/FanoutLimiter.js
curl --connect-timeout 1 --max-time 4 --silent --show-error --output /dev/null --write-out 'root code=%{http_code} total=%{time_total}\n' http://127.0.0.1:10001/
```
