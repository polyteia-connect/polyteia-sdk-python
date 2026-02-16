import uuid
import warnings
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
    "is_true",
    "is_false"
}

VALID_MODES = {
    "queryBuilder",
    "sqlEditor"
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

# V3 specific classes (deprecated)
@dataclass
class QueryBuilderDefV3:
    version: int = 2
    datasets: List[DatasetDef] = field(default_factory=list)
    select: List[SelectDef] = field(default_factory=list)
    where: List[WhereDef] = field(default_factory=list)
    orderBy: List[OrderByDef] = field(default_factory=list)
    limit: Optional[int] = None

@dataclass
class QueryDefV3:
    version: int = 3
    mode: str = "queryBuilder"
    sqlEditor: Dict[str, str] = field(default_factory=lambda: {"sqlString": ""})
    queryBuilder: QueryBuilderDefV3 = field(default_factory=QueryBuilderDefV3)

@dataclass
class InsightDefV3:
    id: Optional[str] = None
    solutionId: str = ""
    name: str = ""
    slug: str = ""
    description: str = ""
    query: QueryDefV3 = field(default_factory=QueryDefV3)
    config: Optional[Dict[str, Any]] = None

# Current version dataclasses
@dataclass
class PivotDef:
    enabled: bool = False
    columns: List[Any] = field(default_factory=list)
    rows: List[Any] = field(default_factory=list)
    values: List[Any] = field(default_factory=list)

@dataclass
class VariableDef:
    id: str
    name: str
    label: str
    type: str
    inputOption: str
    dropdownOption: str
    availableValuesSource: str
    customValues: str
    defaultValue: Optional[str] = None
    alwaysRequired: bool = True

@dataclass
class SqlEditorDef:
    sqlString: str = ""
    variables: List[VariableDef] = field(default_factory=list)

@dataclass
class QueryBuilderDef:
    version: int = 3
    datasets: List[DatasetDef] = field(default_factory=list)
    select: List[SelectDef] = field(default_factory=list)
    where: List[WhereDef] = field(default_factory=list)
    orderBy: List[OrderByDef] = field(default_factory=list)
    pivot: PivotDef = field(default_factory=PivotDef)
    limit: Optional[int] = None

@dataclass
class QueryDef:
    version: int = 4
    mode: str = "queryBuilder"
    sqlEditor: SqlEditorDef = field(default_factory=SqlEditorDef)
    queryBuilder: QueryBuilderDef = field(default_factory=QueryBuilderDef)

@dataclass
class InsightDef:
    id: Optional[str] = None
    solutionId: str = ""
    name: str = ""
    slug: str = ""
    description: str = ""
    query: QueryDef = field(default_factory=QueryDef)
    config: Optional[Dict[str, Any]] = None

class InsightBuilderBase:
    """Base class with shared functionality between current and V3 versions."""
    
    def set_solution_id(self, solution_id: str):
        self._insight.solutionId = solution_id
        return self

    def set_name(self, name: str):
        self._insight.name = name
        return self
    
    def set_slug(self, slug: str):
        self._insight.slug = slug
        return self

    def set_description(self, desc: str):
        self._insight.description = desc
        return self

    def set_mode(self, mode: str):
        if mode not in VALID_MODES:
            raise ValueError(f"Invalid mode: {mode}. Valid modes are: {VALID_MODES}")
        self._insight.query.mode = mode
        return self

    def add_dataset(self, dataset_id: str, join_type: str = "inner", join_on: Optional[List[Dict[str, Any]]] = None):
        ds = DatasetDef(
            datasetId=dataset_id,
            join={"type": join_type, "on": join_on or []}
        )
        self._insight.query.queryBuilder.datasets.append(ds)
        return self

    def add_select(self, column_id: str, dataset_id: Optional[str] = None, aggregate: Optional[str] = None,
                  label: Optional[str] = None, id: Optional[str] = None):
        ds_id = dataset_id or (self._insight.query.queryBuilder.datasets[0].datasetId if self._insight.query.queryBuilder.datasets else "")
        sel = SelectDef(
            id=id or str(uuid.uuid4()),
            datasetId=ds_id,
            columnId=column_id,
            aggregate=aggregate,
            label=label or column_id
        )
        self._insight.query.queryBuilder.select.append(sel)
        return self

    def add_filter(self, column_id: str, operator: str, value: Any, dataset_id: Optional[str] = None):
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

    def add_order_by(self, column_id: str, dataset_id: Optional[str] = None,
                    aggregate: Optional[str] = None, direction: str = "asc"):
        ds_id = dataset_id or (self._insight.query.queryBuilder.datasets[0].datasetId if self._insight.query.queryBuilder.datasets else "")
        ob = OrderByDef(
            id=str(uuid.uuid4()),
            column={"datasetId": ds_id, "columnId": column_id, "aggregate": aggregate},
            direction=direction
        )
        self._insight.query.queryBuilder.orderBy.append(ob)
        return self

    def set_limit(self, limit: int):
        """Set a limit on the number of results returned by the query."""
        if limit < 0:
            raise ValueError("Limit must be a non-negative integer.")
        self._insight.query.queryBuilder.limit = limit
        return self

    def add_filter_defs(self, filters: List[WhereDef]):
        """Bulk-add pre-built WhereDef objects."""
        self._insight.query.queryBuilder.where.extend(filters)
        return self

    def add_select_defs(self, selects: List[SelectDef]):
        """Bulk-add pre-built SelectDef objects."""
        self._insight.query.queryBuilder.select.extend(selects)
        return self

    def set_config(self, cfg: Dict[str, Any]):
        """Config defines the vizualization settings, this might
        need its own builder in the future."""
        self._insight.config = cfg
        return self

    def set_table(self,
                 columns: List[SelectDef],
                 show_header: bool = True,
                 title: str = "",
                 subtitle: str = ""):
        """Configure a table visualization."""
        self._insight.config = {
            "type": "table",
            "subtitle": subtitle,
            "title": title,
            "series": [
                {
                    "column": {
                        "id": col.id,
                        "key": col.columnId,
                        "label": col.label,
                        "type": "number" if "%" in col.label or "anzahl" in col.label.lower() else "text"
                    },
                    "id": f"col_{i}",
                    "sortable": True,
                    "title": {
                        "text": ""
                    }
                } for i, col in enumerate(columns)
            ],
            "filters": []
        }
        return self

    def set_big_number(self, measure_column: SelectDef, aggregate: str = "sum",
                      title: str = "", subtitle: str = ""):
        self._insight.config = {
            "type": "big-number",
            "title": title,
            "subtitle": subtitle,
            "measure": {
                "column": {
                    "id": measure_column.id,
                    "key": measure_column.columnId,
                    "label": measure_column.label,
                    "type": "number"
                },
                "aggregate": aggregate
            },
            "filters": []
        }
        return self

    def set_bar_chart(self, 
                     x_axis_column: SelectDef,
                     y_axis_column: SelectDef,
                     metric_column: Optional[SelectDef] = None,
                     bar_group_type: str = "group",
                     bar_layout: str = "vertical",
                     show_label: bool = True,
                     title: str = "",
                     subtitle: str = "",
                     ticks_layout: str = "normal"):
        """Configure a bar chart visualization."""
        self._insight.config = {
            "type": "bar-chart",
            "barGroupType": bar_group_type,
            "barLayout": bar_layout,
            "showLabel": show_label,
            "title": title,
            "subtitle": subtitle,
            "xAxis": {
                "column": {
                    "id": x_axis_column.id,
                    "key": x_axis_column.columnId,
                    "label": x_axis_column.label,
                    "type": "text"
                },
                "ticksLayout": ticks_layout
            },
            "yAxis": {
                "column": {
                    "id": y_axis_column.id,
                    "key": y_axis_column.columnId,
                    "label": y_axis_column.label,
                    "type": "number"
                }
            },
            "metric": {
                "column": None if metric_column is None else {
                    "id": metric_column.id,
                    "key": metric_column.columnId,
                    "label": metric_column.label,
                    "type": "text"
                }
            },
            "filters": []
        }
        return self

    def set_line_chart(self,
                      x_axis_column: SelectDef,
                      y_axis_column: SelectDef,
                      metric_column: Optional[SelectDef] = None,
                      interpolation: str = "linear",
                      show_label: bool = True,
                      stack: str = "none",
                      title: str = "",
                      subtitle: str = "",
                      ticks_layout: str = "normal"):
        """Configure a line chart visualization."""
        self._insight.config = {
            "type": "line-chart",
            "lineInterpolation": interpolation,
            "showLabel": show_label,
            "title": title,
            "subtitle": subtitle,
            "stack": stack,
            "xAxis": {
                "column": {
                    "id": x_axis_column.id,
                    "key": x_axis_column.columnId,
                    "label": x_axis_column.label,
                    "type": "text"
                },
                "ticksLayout": ticks_layout
            },
            "yAxis": {
                "column": {
                    "id": y_axis_column.id,
                    "key": y_axis_column.columnId,
                    "label": y_axis_column.label,
                    "type": "number"
                }
            },
            "metric": {
                "column": None if metric_column is None else {
                    "id": metric_column.id,
                    "key": metric_column.columnId,
                    "label": metric_column.label,
                    "type": "text"
                }
            },
            "filters": []
        }
        return self

    def set_pie_chart(self,
                     label_column: SelectDef,
                     measure_column: SelectDef,
                     appearance: str = "pie",
                     title: str = "",
                     subtitle: str = ""):
        """Configure a pie chart visualization."""
        self._insight.config = {
            "type": "pie-chart",
            "appearance": appearance,
            "title": title,
            "subtitle": subtitle,
            "label": {
                "column": {
                    "id": label_column.id,
                    "key": label_column.columnId,
                    "label": label_column.label,
                    "type": "text"
                }
            },
            "measure": {
                "column": {
                    "id": measure_column.id,
                    "key": measure_column.columnId,
                    "label": measure_column.label,
                    "type": "number"
                }
            },
            "filters": []
        }
        return self

    def set_map_chart(self,
                     geometry_column: SelectDef,
                     label_column: Optional[SelectDef] = None,
                     value_column: Optional[SelectDef] = None,
                     show_label: bool = True,
                     title: str = "",
                     subtitle: str = "",
                     layer_type: str = "choropleth",
                     layer_title: str = "",
                     fill_style: str = "opaque",
                     background_map: str = "osm",
                     enable_feature_grouping: Optional[bool] = None,
                     group_column: Optional[SelectDef] = None):
        """Configure a map chart visualization."""
        layer = {
            "type": layer_type,
            "fillStyle": fill_style,
            "id": str(uuid.uuid4()).replace("-", ""),
            "showLabel": show_label,
            "title": layer_title,
            "tooltip": {"fields": None},
            "geometryColumn": {
                "id": geometry_column.id,
                "key": geometry_column.columnId,
                "label": geometry_column.label,
                "type": "text"
            }
        }

        if label_column:
            layer["labelColumn"] = {
                "id": label_column.id,
                "key": label_column.columnId,
                "label": label_column.label,
                "type": "text"
            }

        if value_column:
            layer["valueColumn"] = {
                "id": value_column.id,
                "key": value_column.columnId,
                "label": value_column.label,
                "type": "number"
            }

        if layer_type == "scatter":
            layer["enableFeatureGrouping"] = enable_feature_grouping if enable_feature_grouping is not None else False
            layer["groupColumn"] = None
            if group_column:
                layer["groupColumn"] = {
                    "id": group_column.id,
                    "key": group_column.columnId,
                    "label": group_column.label,
                    "type": "text"
                }

        self._insight.config = {
            "type": "map-chart",
            "title": title,
            "subtitle": subtitle,
            "backgroundMap": background_map,
            "version": 2,
            "layers": [layer],
            "filters": []
        }
        return self


class InsightBuilderV3(InsightBuilderBase):
    """Version 3 of the InsightBuilder (Deprecated)."""
    
    def __init__(self):
        warnings.warn(
            "InsightBuilderV3 is deprecated and will be removed in a future version. "
            "Please use InsightBuilder instead.",
            DeprecationWarning,
            stacklevel=2
        )
        self._insight = InsightDefV3()

    def set_sql(self, sql: str) -> 'InsightBuilderV3':
        self._insight.query.sqlEditor["sqlString"] = sql
        return self

    def build(self) -> Dict[str, Any]:
        insight = {
            "solution_id": self._insight.solutionId,
            "name": self._insight.name,
            "description": self._insight.description,
            "slug": self._insight.slug,
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


class InsightBuilder(InsightBuilderBase):
    """Current version of the InsightBuilder."""
    
    def __init__(self):
        self._insight = InsightDef()

    def set_sql(self, sql: str) -> 'InsightBuilder':
        self._insight.query.sqlEditor.sqlString = sql
        return self

    def add_sql_variable(self,
                        id: str,
                        name: str,
                        label: str,
                        type: str = "text",
                        input_option: str = "dropdown",
                        dropdown_option: str = "single",
                        available_values_source: str = "custom",
                        custom_values: str = "",
                        default_value: Optional[str] = None,
                        always_required: bool = True
    ) -> 'InsightBuilder':
        """Add a variable to the SQL query."""
        var = VariableDef(
            id=id,
            name=name,
            label=label,
            type=type,
            inputOption=input_option,
            dropdownOption=dropdown_option,
            availableValuesSource=available_values_source,
            customValues=custom_values,
            defaultValue=default_value,
            alwaysRequired=always_required
        )
        self._insight.query.sqlEditor.variables.append(var)
        return self

    def build(self) -> Dict[str, Any]:
        insight = {
            "solution_id": self._insight.solutionId,
            "name": self._insight.name,
            "description": self._insight.description,
            "slug": self._insight.slug,
            "query": {
                "version": self._insight.query.version,
                "mode": self._insight.query.mode,
                "sqlEditor": {
                    "sqlString": self._insight.query.sqlEditor.sqlString,
                    "variables": [
                        {
                            "id": v.id,
                            "name": v.name,
                            "label": v.label,
                            "type": v.type,
                            "inputOption": v.inputOption,
                            "dropdownOption": v.dropdownOption,
                            "availableValuesSource": v.availableValuesSource,
                            "customValues": v.customValues,
                            "defaultValue": v.defaultValue,
                            "alwaysRequired": v.alwaysRequired
                        } for v in self._insight.query.sqlEditor.variables
                    ]
                },
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
                    "pivot": {
                        "enabled": self._insight.query.queryBuilder.pivot.enabled,
                        "columns": self._insight.query.queryBuilder.pivot.columns,
                        "rows": self._insight.query.queryBuilder.pivot.rows,
                        "values": self._insight.query.queryBuilder.pivot.values
                    },
                    "limit": self._insight.query.queryBuilder.limit
                }
            },
            "config": self._insight.config
        }
        return insight
