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
        self.sum_control = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.amount_by_user = {}        

        self.data_output_exchanges = []
```

- `amount_by_user` almacena un diccionario con uuid de usuario y por cada uno un diccionario de frutas cantidad, con la finalidad de que no se mezclen las cantidades de los distintos usuarios.

Como la recepción de mensajes por parte de los `SumFilter` es mediante una cola estilo productor-consumidor el mensaje de EOF de un cliente llega a un solo `SumFilter`. Para solucionar esta situación la instancia de `SumFilter` que recibe el EOF hace lo siguiente:

``` Python
 for i in range(SUM_AMOUNT-1):
    self.sum_control.send(message_protocol.internal.serialize([userId]))
```
Al recibir el primer EOF reenvia `SUM_AMOUNT` veces el mensaje a la misma cola. Si vuelve a recibir EOF, simplemente le niega la recepción del EOF al MOM con un `nack()`.

Luego de recibir el EOF, por cada fruta se elige el nodo de aggregation determinado por el nombre de la fruta interpretada como su valor en ASCII. Como la función `hash` no es deterministica no se podia utilizar en este caso porque se iba a distribuir en distintas instancias de `AggregationFilter` la información de una misma fruta. Para solucionar esto, se aprovecho que los valores ASCII son unicos.

``` Python
for final_fruit_item in self.amount_by_user[userId].values():   
    node = int.from_bytes(final_fruit_item.fruit.encode("ASCII"), byteorder='big', signed=False) % AGGREGATION_AMOUNT
    self.data_output_exchanges[node].send(message_protocol.internal.serialize([final_fruit_item.fruit, final_fruit_item.amount, userId]))
```

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
- `fruit_top` tiene la misma funcionalidad que `amount_by_user` pero generando tops parciales
- `eof` almacena la cantidad de EOF recibidos por uuid

Si la cantidad de EOF es igual a `SUM_AMOUNT` ocurre lo siguiente:

``` Python
if self.eof[userId] == SUM_AMOUNT:
  fruit_chunk = list(self.fruit_top[userId][-TOP_SIZE:])
  fruit_chunk.reverse()
  fruit_top = list(map(lambda fruit_item: (fruit_item.fruit, fruit_item.amount), fruit_chunk))
  self.output_queue.send(message_protocol.internal.serialize(fruit_top))
```

Join
---- 
Para poder coordinar el procesamiento del servicio Join se implemento la siguiente clase `JoinFilter`: 


``` Python
def __init__(self):
    self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
    self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    self.top = {}
    self.eof = {}
```

- `fruit_top` tiene la misma funcionalidad que `amount_by_user` pero con los tops globales
- `eof` almacena la cantidad de EOF recibidos por uuid

Si la cantidad de EOF es igual a `AGGREGATION_AMOUNT` ocurre lo siguiente:

``` Python
if self.eof[userId] == AGGREGATION_AMOUNT:
    fruit_chunk = list(self.top[userId][-TOP_SIZE:])
    fruit_chunk.reverse()
    top = list(map(lambda fruit_item: (fruit_item.fruit, fruit_item.amount), fruit_chunk))

    top.append(userId)

    self.output_queue.send(message_protocol.internal.serialize(top))
```
