# Santiago Henseler 110732
Trabajo práctico N°2: Coordinación
----
El objetivo del trabajo práctico fue implementar la coordinación entre varias computadoras escaladas horizontalmente.

Sum
---- 
Para poder coordinar el procesamiento del servicio Sum se implemento la siguiente clase `SumFilter`: 

``` Python
class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.amount_by_user = {}        
        self.sum_control = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.eof = {}

        self.data_output_exchanges = []
```

- `amount_by_user` almacena un diccionario con uuid de usuario y por cada uno un diccionario de frutas cantidad, con la finalidad de que no se mezclen las cantidades de los distintos usuarios.
- `eof` almacena los uuid de los usuarios que ya enviaron EOF.

Como la recepción de mensajes por parte de los `SumFilter` es mediante una cola estilo productor-consumidor el mensaje de EOF de un cliente llega a un solo `SumFilter`. Para solucionar esta situación la instancia de `SumFilter` que recibe el EOF hace lo siguiente:

``` Python
if userId in self.eof:
  if self.eof[userId] != SUM_AMOUNT-1:
    self.sum_control.send(message_protocol.internal.serialize([userId]))
    self.eof[userId] += 1
  return

[...]

 for i in range(SUM_AMOUNT-1):
    self.sum_control.send(message_protocol.internal.serialize([userId]))
```
Al recibir el primer EOF reenvia `SUM_AMOUNT` veces el mensaje a la misma cola. Si vuelve a recibir EOF, simplemente lo vuelve a reenviar una vez a la cola. Si ya lo reenvio `SUM_AMOUNT` veces se asume que ya todas las instancias recibieron el EOF.

Luego de recibir el EOF se elige el nodo determinado por el uuid y se envia toda la información

``` Python
node =  int(userId.replace("-", ""), 16) % AGGREGATION_AMOUNT    

for final_fruit_item in self.amount_by_user[userId].values():
  self.data_output_exchanges[node].send(message_protocol.internal.serialize([final_fruit_item.fruit, final_fruit_item.amount, userId]))
        
self.data_output_exchanges[node].send(message_protocol.internal.serialize([userId]))
```
Como la función `hash` no es deterministica no se podia utilizar en este caso porque se iba a distribuir en distintas instancias de `AggregationFilter` la información de un mismo usuario. Para solucionar esto, se aprovecho que el uuid es unico para cada usuario y es representado en hexadecimal. Simplemente se transforma a base 10 y luego se aplica modulo `AGGREGATION_AMOUNT`.    

Aggregation
---- 
Para poder coordinar el procesamiento del servicio Aggregation se implemento la siguiente clase `AggregationFilter`: 

``` Python
def __init__(self):
  self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"])
  self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
  self.fruit_top = {}
  self.eof = {}
```
- `fruit_top` tiene la misma funcionalidad que `amount_by_user` 
- `eof` almacena la cantidad de EOF recibidos por uuid

Si la cantidad de EOF es igual a `SUM_AMOUNT` ocurre lo siguiente:

``` Python
if self.eof[userId] == SUM_AMOUNT:
  fruit_chunk = list(self.fruit_top[userId][-TOP_SIZE:])
  fruit_chunk.reverse()
  fruit_top = list(map(lambda fruit_item: (fruit_item.fruit, fruit_item.amount), fruit_chunk))
  self.output_queue.send(message_protocol.internal.serialize(fruit_top))
```