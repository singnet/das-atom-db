# Índices

## Índices Simples

Índices simples são úteis quando você tem consultas frequentes que envolvem um único campo. Aqui estão alguns exemplos e suas respectivas utilizações:

### Índice no Campo "name" em Ordem Decrescente

```python
simple_node_indexes = [
    Index("name", OrderEnum.DESC), 
]
```

Este índice é útil quando você faz consultas frequentes envolvendo o campo "name". Ele indexará o campo "name" de maneira decrescente, o que pode acelerar consultas que precisam dos resultados ordenados de forma decrescente por esse campo.

#### Exemplo de Uso:
```mongodb
db.nodes.find({ name: "human" })
```

### Índice no Campo "named_type" em Ordem Crescente

```python
simple_node_indexes = [
    Index("named_type"), 
]
```

Este índice é útil para consultas frequentes envolvendo o campo "named_type". Ele indexará o campo "named_type" de maneira ascendente, permitindo consultas eficientes que precisam dos resultados ordenados de forma crescente por esse campo.

#### Exemplo de Uso:
```mongodb
db.nodes.find({ named_type: "Concept" })
```

## Índices Compostos

Índices compostos são criados com base em vários campos. Os dados são agrupados pelo primeiro campo no índice e, em seguida, por cada campo subsequente. Veja um exemplo:

### Índice Composto nos Campos "name" e "named_type"

```python
compound_node_indexes = [
    CompoundIndex(indexes=[
        Index("name", OrderEnum.DESC),
        Index("named_type"),
    ])
]
```

Este índice é útil para consultas que envolvem os campos "name" e "named_type". Ele primeiro ordena os documentos pelo campo "name" de forma decrescente e, em seguida, pelos valores do campo "named_type". Isso pode melhorar o desempenho de consultas que envolvem esses critérios de pesquisa.

#### Exemplo de Uso:
```mongodb
db.nodes.find({ name: "human", "named_type": "Concept" })
```

## Índices com Expressão de Filtro Parcial

Índices com expressão de filtro parcial são úteis quando você deseja otimizar consultas específicas, excluindo documentos que não atendem a determinados critérios de filtragem. Veja um exemplo:

### Índice com Expressão de Filtro Parcial no Campo "purchaseMethod"

```python
partial_filter_expression_node_indexes = [
    Index("purchaseMethod",
        partial_filter_expression=PartialFilterExpression(
            field_name="customer.age",
            operations={
                PartialFilterExpressionOptions.GREATER_EQUAL_THAN: 18,
                PartialFilterExpressionOptions.LESS_EQUAL_THAN: 30
            }
        ),
    ),
]
```

Este índice indexa o campo "purchaseMethod" apenas para transações de clientes com idades entre 18 e 30 anos. Isso pode melhorar o desempenho de consultas que envolvem esse critério de filtro.

#### Exemplo de Uso:
```mongodb
db.sales.find({ purchaseMethod: "Online", "customer.age": { $gte: 19, $lte: 25 } })
```

## Índices em Arrays de Objetos

Quando você tem um array de objetos em seus documentos e deseja indexar um campo dentro desses objetos, pode usar a notação de ponto (`.`) para especificar o caminho completo até o campo desejado. Veja um exemplo:

### Índice no Campo "name" Dentro do Array "items"

```python
array_object_node_indexes = [
    Index("items.name"),
]
```

Este índice indexa o campo "name" dentro do array "items". Isso pode ser útil quando você precisa consultar ou classificar documentos com base nos valores de campos dentro de arrays de objetos.

#### Exemplo de Uso:
```mongodb
db.sales.find({ "items.name": "item01" })
```

