# Informe — TP Coordinación

## Punto de partida

Tras la lectura de la consigna y la revisión del código base, se identificaron una serie de problemas de coordinación. Para que queden trazables de punta a punta se los recorre con un mismo ejemplo guía, con dos clientes concurrentes y dos réplicas de cada control:

- **Cliente A** envía `(manzana, 5)`, `(banana, 3)` y luego EOF.
- **Cliente B** envía `(manzana, 10)`, `(pera, 7)` y luego EOF.
- `SUM_AMOUNT=2` (`sum_0`, `sum_1`) y `AGGREGATION_AMOUNT=2` (`aggregation_0`, `aggregation_1`).

### Middleware sin implementar

`common/middleware/middleware_rabbitmq.py` tenía ambos constructores vacíos (`pass`). No había transporte real entre procesos, así que ninguno de los pasos del ejemplo guía llegaba siquiera a ejecutarse: el sistema no levantaba.

**Solución:** se implementaron ambas clases sobre `pika.BlockingConnection`, con dos formas de comunicación distintas según lo que necesita cada salto de la tubería:

- `MessageMiddlewareQueueRabbitMQ`: una cola durable de trabajo, con consumidores en competencia (round-robin), queremos repartir carga entre réplicas, los datos de `gateway` hacia `sum`.
- `MessageMiddlewareExchangeRabbitMQ`: un exchange de RabbitMQ que se comporta de dos maneras según si se le pasan o no *routing keys*:
  - **Con keys** (tipo `direct`): cada instancia se bindea a su propia clave (p. ej. `aggregation_0`), lo que habilita mandarle un mensaje a una única instancia puntual en vez de a todas.
  - **Sin keys** (tipo `fanout`): cada instancia que se conecta arma su propia cola anónima y exclusiva bindeada al exchange, así que todas reciben una copia de lo publicado sin que el publisher necesite saber cuántas son, esta es la base para resolver "El EOF de un cliente no llegaba a todas las réplicas de Sum", justamente porque el `gateway` no tiene forma de conocer `SUM_AMOUNT`.


Esto se validó contra el escenario 1 (`make switch` → 1 → `make test`): un cliente, una sola réplica de cada control, el transporte funciona de punta a punta.

### Sin aislamiento por cliente

El protocolo interno serializaba únicamente `(fruta, cantidad)`, sin ningún identificador de cliente. `sum`, `aggregation` y `join` acumulan en una única estructura de estado global (`amount_by_fruit`, `fruit_top`), compartida por todas las conexiones activas.

```mermaid
sequenceDiagram
    participant A as Cliente A
    participant B as Cliente B
    participant GW as Gateway
    participant Q as INPUT_QUEUE
    participant S0 as sum_0

    A->>GW: (manzana, 5)
    GW->>Q: publish (manzana, 5)
    B->>GW: (manzana, 10)
    GW->>Q: publish (manzana, 10)
    Q-->>S0: (manzana, 5)
    Q-->>S0: (manzana, 10)
    Note over S0: un solo mapa global:<br/>amount_by_fruit["manzana"] = 5 + 10 = 15<br/>(A y B ya quedaron indistinguibles)
```

**Solución:** el protocolo interno (`common/message_protocol/internal.py`) pasó a ser un *envelope* `{client_id, type, payload}` (para `MsgType.DATA` y `MsgType.EOF`), en vez de una lista `(fruta, cantidad)` a secas. El `gateway` genera un `client_id` (`uuid.uuid4()`) por cada conexión aceptada y se lo pasa al `MessageHandler` de esa conexión, que lo mete en todo mensaje que arma. `sum` y `aggregation` dejaron de tener un único estado global: pasaron a `amount_by_fruit_by_client` y `fruit_top_by_client` respectivamente (dict de `client_id` → estado), así que dos clientes concurrentes ya no comparten acumulador, sino que cada uno arma y flushea el suyo de forma independiente, sin pisarse. 

Validado contra el escenario 2 (`make switch` → 2 → `make test`): tres clientes concurrentes, una sola réplica de cada control y cada cliente recibe su propio top, sin mezcla.

### El EOF de un cliente no llegaba a todas las réplicas de Sum

El `gateway` publicaba el aviso de fin de ingesta en la misma cola de trabajo (`INPUT_QUEUE`) que los datos. Como `sum_0` y `sum_1` son consumidores en competencia de esa cola, ese único mensaje lo recibía sólo uno de los dos; el otro nunca se enteraba de que el cliente había terminado y lo que tenga acumulado para ese cliente no se llegaba a enviar. La constante `SUM_CONTROL_EXCHANGE`, declarada pero no usada en `sum/main.py`, era indicio de que el esqueleto anticipaba un canal de control separado para esto.

```mermaid
sequenceDiagram
    participant GW as Gateway
    participant Q as INPUT_QUEUE
    participant S0 as sum_0
    participant S1 as sum_1

    Note over Q,S1: sum_1 tiene guardado (banana, 3) de A
    GW->>Q: publish EOF (cliente A)
    Q-->>S0: EOF (cliente A)
    Note over S0: sum_0 flushea y sigue de largo
    Note over S1: sum_1 nunca recibe este EOF:<br/>(banana, 3) de A queda huérfano
```

**Solución:** pendiente — incremento 3 (fan-out del EOF Gateway→Sum sobre el exchange `fanout` ya implementado).

### Sin partición de datos entre réplicas de Aggregation

En `sum/main.py`, `_process_eof` reenviaba cada fruta a **todas** las instancias de Aggregation (loop anidado sobre `data_output_exchanges`), en lugar de a una sola. Esto multiplicaba por `AGGREGATION_AMOUNT` tanto el tráfico entre controles como el cómputo, porque cada Aggregation vuelvía a sumar y ordenar datos que ya había procesado la otra, violando el requisito de minimizar redundancia.

```mermaid
flowchart LR
    S0[sum_0] -->|manzana: 15| A0[aggregation_0]
    S0 -->|manzana: 15 duplicado| A1[aggregation_1]
```

### Sin barrera de fin de ingesta en Aggregation

`aggregation/main.py` calculaba y emitía su top parcial apenas recibía el primer mensaje de EOF, sin contar que, con `SUM_AMOUNT` instancias de Sum enviándole datos, tendría que haber recibido un EOF de cada una antes de saber que ya tiene el total definitivo de sus frutas.

```mermaid
sequenceDiagram
    participant S0 as sum_0
    participant S1 as sum_1
    participant A0 as aggregation_0

    S0->>A0: manzana: 15
    S0->>A0: EOF
    Note over A0: cierra y emite el top<br/>con lo que tiene hasta acá
    S1--xA0: (nunca llegó a mandar nada,<br/>ver problema del EOF de Sum)
```

### Sin combinación real en Join

`join/main.py` era un *passthrough*: reenviaba cada mensaje recibido tal cual, sin esperar los `AGGREGATION_AMOUNT` EOFs de las instancias de Aggregation ni combinar sus tops parciales en un top final.

```mermaid
sequenceDiagram
    participant A0 as aggregation_0
    participant A1 as aggregation_1
    participant J as join
    participant GW as Gateway

    A0->>J: top parcial (incompleto, ver arriba)
    J->>GW: lo reenvía tal cual
    Note over J: no esperó el top de aggregation_1
```

### El gateway no podía enrutar resultados a su cliente de origen

`message_handler.py` no tenía ninguna noción de a qué cliente pertenecía un mensaje de resultado; simplemente lo ofrecía al primer cliente conectado que no descartara el mensaje.

```mermaid
sequenceDiagram
    participant GW as Gateway
    participant A as Cliente A
    participant B as Cliente B

    Note over GW: llega el resultado (mezclado e<br/>incompleto) que en verdad era de A
    GW->>B: FRUIT_TOP
    Note over B: cliente B recibe un resultado<br/>que ni siquiera es el suyo
```

**Solución:** `MessageHandler` ahora guarda el `client_id` de su conexión (recibido al construirse) y `deserialize_result_message` sólo devuelve el payload si el `client_id` del mensaje coincide con el propio; si no, devuelve `None` para que el loop de `handle_client_response` siga probando con el resto de los clientes conectados. Esto reemplaza el "se lo doy al primero que no tire error" por un enrutamiento real por identidad.

Validado junto con el ítem anterior contra el escenario 2: cada uno de los tres clientes recibe únicamente su propio resultado.

### Sin manejo de señales en los controles internos

`sum`, `aggregation` y `join` no capturaban SIGTERM (sólo lo hacían `client` y `gateway`). Si en medio de esta corrida se ejecutaba `make down`, `sum_1` —que todavía tenía `(banana, 3)` de A sin flushear— era terminado abruptamente al vencer el plazo de gracia, sin poder cerrar la conexión al middleware de forma ordenada.
