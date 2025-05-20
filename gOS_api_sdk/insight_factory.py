import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

VALID_FILTER_OPERATORS = {
    "equals",
    "not_equals",
    "like",
    "not_like",
    "starts_with",
    "ends_with",
    "contains",
    "not_contains",
    "is_null",
    "is_not_null",
    "greater_than",
    "less_than",
    "greater_or_equals",
    "less_or_equals",
    "is_null_or_empty",
    "is_not_null_or_empty",
}

@dataclass
class DatasetDef:
    datasetId: str
    join: Dict[str, Any] = field(default_factory=lambda: {"type": "inner", "on": []})

@dataclass
class SelectDef:
    id: str
    datasetId: str
    columnId: str
    aggregate: Optional[str]
    label: str

@dataclass
class WhereDef:
    id: str
    column: Dict[str, Any]
    operator: str
    value: Any

@dataclass
class OrderByDef:
    id: str
    column: Dict[str, Any]
    direction: str

@dataclass
class QueryBuilderDef:
    version: int = 2
    datasets: List[DatasetDef] = field(default_factory=list)
    select: List[SelectDef] = field(default_factory=list)
    where: List[WhereDef] = field(default_factory=list)
    orderBy: List[OrderByDef] = field(default_factory=list)
    limit: Optional[int] = None

@dataclass
class QueryDef:
    version: int = 3
    mode: str = "queryBuilder"
    sqlEditor: Dict[str, str] = field(default_factory=lambda: {"sqlString": ""})
    queryBuilder: QueryBuilderDef = field(default_factory=QueryBuilderDef)

@dataclass
class InsightDef:
    id: Optional[str] = None
    solutionId: str = ""
    name: str = ""
    description: str = ""
    query: QueryDef = field(default_factory=QueryDef)
    config: Optional[Dict[str, Any]] = None


class InsightBuilderV3:
    def __init__(self):
        self._insight = InsightDef()

    def set_solution_id(self, solution_id: str) -> 'InsightBuilderV3':
        self._insight.solutionId = solution_id
        return self

    def set_name(self, name: str) -> 'InsightBuilderV3':
        self._insight.name = name
        return self

    def set_description(self, desc: str) -> 'InsightBuilderV3':
        self._insight.description = desc
        return self

    def set_mode(self, mode: str) -> 'InsightBuilderV3':
        """This could be either 'queryBuilder' or 'sqlEditor'."""
        self._insight.query.mode = mode
        return self

    def set_sql(self, sql: str) -> 'InsightBuilderV3':
        self._insight.query.sqlEditor["sqlString"] = sql
        return self

    def add_dataset(self,
                    dataset_id: str,
                    join_type: str = "inner",
                    join_on: Optional[List[Dict[str, Any]]] = None
    ) -> 'InsightBuilderV3':
        ds = DatasetDef(
            datasetId=dataset_id,
            join={"type": join_type, "on": join_on or []}
        )
        self._insight.query.queryBuilder.datasets.append(ds)
        return self

    def add_select(self,
                   column_id: str,
                   dataset_id: Optional[str] = None,
                   aggregate: Optional[str] = None,
                   label: Optional[str] = None
    ) -> 'InsightBuilderV3':
        ds_id = dataset_id or (self._insight.query.queryBuilder.datasets[0].datasetId if self._insight.query.queryBuilder.datasets else "")
        sel = SelectDef(
            id=str(uuid.uuid4()),
            datasetId=ds_id,
            columnId=column_id,
            aggregate=aggregate,
            label=label or column_id
        )
        self._insight.query.queryBuilder.select.append(sel)
        return self

    def add_filter(self,
                   column_id: str,
                   operator: str,
                   value: Any,
                   dataset_id: Optional[str] = None
    ) -> 'InsightBuilderV3':
        if operator not in VALID_FILTER_OPERATORS:
            raise ValueError(f"Invalid operator: {operator}. Valid operators are: {VALID_FILTER_OPERATORS}")
        ds_id = dataset_id or (self._insight.query.queryBuilder.datasets[0].datasetId if self._insight.query.queryBuilder.datasets else "")
        where = WhereDef(
            id=str(uuid.uuid4()),
            column={"datasetId": ds_id, "columnId": column_id, "aggregate": None},
            operator=operator,
            value=value
        )
        self._insight.query.queryBuilder.where.append(where)
        return self

    def add_order_by(self,
                     column_id: str,
                     dataset_id: Optional[str] = None,
                     aggregate: Optional[str] = None,
                     direction: str = "asc"
    ) -> 'InsightBuilderV3':
        ds_id = dataset_id or (self._insight.query.queryBuilder.datasets[0].datasetId if self._insight.query.queryBuilder.datasets else "")
        ob = OrderByDef(
            id=str(uuid.uuid4()),
            column={"datasetId": ds_id, "columnId": column_id, "aggregate": aggregate},
            direction=direction
        )
        self._insight.query.queryBuilder.orderBy.append(ob)
        return self
    
    def add_filter_defs(self, filters: List[WhereDef]) -> 'InsightBuilderV3':
        """Bulk-add pre-built WhereDef objects."""
        self._insight.query.queryBuilder.where.extend(filters)
        return self

    def add_select_defs(self, selects: List[SelectDef]) -> 'InsightBuilderV3':
        """Bulk-add pre-built SelectDef objects."""
        self._insight.query.queryBuilder.select.extend(selects)
        return self

    def set_limit(self, limit: int) -> 'InsightBuilderV3':
        self._insight.query.queryBuilder.limit = limit
        return self

    def set_config(self, cfg: Dict[str, Any]) -> 'InsightBuilderV3':
        """Config defines the vizualization settings, this might
        need its own builder in the future."""
        self._insight.config = cfg
        return self

    def build(self) -> Dict[str, Any]:
        # Convert dataclasses to dicts
        insight = {
            "solution_id": self._insight.solutionId,
            "name": self._insight.name,
            "description": self._insight.description,
            "query": {
                "version": self._insight.query.version,
                "mode": self._insight.query.mode,
                "sqlEditor": self._insight.query.sqlEditor,
                "queryBuilder": {
                    "version": self._insight.query.queryBuilder.version,
                    "datasets": [vars(ds) for ds in self._insight.query.queryBuilder.datasets],
                    "select": [vars(s) for s in self._insight.query.queryBuilder.select],
                    "where": [
                        {
                            "id": w.id,
                            "column": w.column,
                            "operator": w.operator,
                            "value": w.value
                        } for w in self._insight.query.queryBuilder.where
                    ],
                    "orderBy": [
                        {
                            "id": o.id,
                            "column": o.column,
                            "direction": o.direction
                        } for o in self._insight.query.queryBuilder.orderBy
                    ],
                    "limit": self._insight.query.queryBuilder.limit
                }
            },
            "config": self._insight.config
        }
        return insight
