"""Builders for insight definitions.

The query side (datasets / select / where / orderBy / pivot / SQL editor) is
unchanged from the previous major version. The chart ``config`` side emits the
current visualization schema (discriminated on ``type``).
"""

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

VALID_FILTER_OPERATORS = {
    "equals", "not_equals", "like", "not_like", "starts_with", "ends_with",
    "contains", "not_contains", "is_null", "is_not_null", "greater_than",
    "less_than", "greater_or_equals", "less_or_equals", "is_null_or_empty",
    "is_not_null_or_empty", "is_true", "is_false",
}

VALID_MODES = {"queryBuilder", "sqlEditor"}


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
    organizationId: str = ""
    name: str = ""
    slug: str = ""
    description: str = ""
    query: QueryDef = field(default_factory=QueryDef)
    config: Optional[Dict[str, Any]] = None


def _col(select: "SelectDef", type_hint: str) -> Dict[str, Any]:
    """Build a visualization column reference {id, key, label, type} from a
    SelectDef. ``type_hint`` is the column's data type (e.g. 'number', 'text')."""
    return {
        "id": select.id,
        "key": select.columnId,
        "label": select.label,
        "type": type_hint,
    }


class InsightBuilder:
    """Fluent builder for an insight definition (query + chart config)."""

    def __init__(self):
        self._insight = InsightDef()

    # --- identity / query -------------------------------------------------

    def set_solution_id(self, solution_id: str):
        self._insight.solutionId = solution_id
        return self

    def set_organization_id(self, organization_id: str):
        self._insight.organizationId = organization_id
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

    def set_sql(self, sql: str):
        self._insight.query.sqlEditor.sqlString = sql
        return self

    def add_sql_variable(self, id: str, name: str, label: str, type: str = "text",
                         input_option: str = "dropdown", dropdown_option: str = "single",
                         available_values_source: str = "custom", custom_values: str = "",
                         default_value: Optional[str] = None, always_required: bool = True):
        self._insight.query.sqlEditor.variables.append(VariableDef(
            id=id, name=name, label=label, type=type, inputOption=input_option,
            dropdownOption=dropdown_option, availableValuesSource=available_values_source,
            customValues=custom_values, defaultValue=default_value, alwaysRequired=always_required,
        ))
        return self

    def add_dataset(self, dataset_id: str, join_type: str = "inner", join_on: Optional[List[Dict[str, Any]]] = None):
        self._insight.query.queryBuilder.datasets.append(
            DatasetDef(datasetId=dataset_id, join={"type": join_type, "on": join_on or []})
        )
        return self

    def _default_ds(self) -> str:
        dss = self._insight.query.queryBuilder.datasets
        return dss[0].datasetId if dss else ""

    def add_select(self, column_id: str, dataset_id: Optional[str] = None, aggregate: Optional[str] = None,
                  label: Optional[str] = None, id: Optional[str] = None):
        self._insight.query.queryBuilder.select.append(SelectDef(
            id=id or str(uuid.uuid4()), datasetId=dataset_id or self._default_ds(),
            columnId=column_id, aggregate=aggregate, label=label or column_id,
        ))
        return self

    def add_filter(self, column_id: str, operator: str, value: Any, dataset_id: Optional[str] = None):
        if operator not in VALID_FILTER_OPERATORS:
            raise ValueError(f"Invalid operator: {operator}. Valid operators are: {VALID_FILTER_OPERATORS}")
        self._insight.query.queryBuilder.where.append(WhereDef(
            id=str(uuid.uuid4()),
            column={"datasetId": dataset_id or self._default_ds(), "columnId": column_id, "aggregate": None},
            operator=operator, value=value,
        ))
        return self

    def add_order_by(self, column_id: str, dataset_id: Optional[str] = None, aggregate: Optional[str] = None, direction: str = "asc"):
        self._insight.query.queryBuilder.orderBy.append(OrderByDef(
            id=str(uuid.uuid4()),
            column={"datasetId": dataset_id or self._default_ds(), "columnId": column_id, "aggregate": aggregate},
            direction=direction,
        ))
        return self

    def set_limit(self, limit: int):
        if limit < 0:
            raise ValueError("Limit must be a non-negative integer.")
        self._insight.query.queryBuilder.limit = limit
        return self

    def add_filter_defs(self, filters: List[WhereDef]):
        self._insight.query.queryBuilder.where.extend(filters)
        return self

    def add_select_defs(self, selects: List[SelectDef]):
        self._insight.query.queryBuilder.select.extend(selects)
        return self

    # --- chart config -----------------------------------------------------

    def set_config(self, cfg: Dict[str, Any]):
        """Set the chart ``config`` object directly."""
        self._insight.config = cfg
        return self

    @staticmethod
    def _common(title: str, subtitle: str) -> Dict[str, Any]:
        return {"title": title, "subtitle": subtitle, "infoText": None, "filters": []}

    def set_table(self, columns: List[SelectDef], show_header: bool = True, title: str = "", subtitle: str = ""):
        cfg = self._common(title, subtitle)
        cfg.update({
            "type": "table",
            "series": [
                {
                    "id": f"col_{i}",
                    "column": _col(col, "number" if ("%" in col.label or "anzahl" in col.label.lower()) else "text"),
                    "title": {"text": ""},
                    "colorize": None,
                    "colorizeStyle": None,
                    "sortable": True,
                }
                for i, col in enumerate(columns)
            ],
        })
        self._insight.config = cfg
        return self

    def set_big_number(self, measure_column: SelectDef, aggregate: str = "sum", title: str = "", subtitle: str = ""):
        cfg = self._common(title, subtitle)
        cfg.update({
            "type": "big-number",
            "measure": {"column": _col(measure_column, "number"), "aggregate": aggregate, "formatting": None},
        })
        self._insight.config = cfg
        return self

    def set_bar_chart(self, x_axis_column: SelectDef, y_axis_column: SelectDef,
                     metric_column: Optional[SelectDef] = None, bar_group_type: str = "group",
                     bar_layout: str = "vertical", show_label: bool = True, title: str = "",
                     subtitle: str = "", ticks_layout: str = "normal"):
        cfg = self._common(title, subtitle)
        cfg.update({
            "type": "bar-chart",
            "xAxis": {"column": _col(x_axis_column, "text"), "ticksLayout": ticks_layout, "formatting": None},
            "yAxis": [{"id": str(uuid.uuid4()), "column": _col(y_axis_column, "number")}],
            "metric": {"column": None if metric_column is None else _col(metric_column, "text")},
            "tooltip": {"fields": []},
            "barGroupType": bar_group_type,
            "barLayout": bar_layout,
            "showLabel": show_label,
        })
        self._insight.config = cfg
        return self

    def set_line_chart(self, x_axis_column: SelectDef, y_axis_column: SelectDef,
                      metric_column: Optional[SelectDef] = None, interpolation: str = "linear",
                      show_label: bool = True, stack: str = "none", title: str = "",
                      subtitle: str = "", ticks_layout: str = "normal", fallback_value: str = "empty"):
        cfg = self._common(title, subtitle)
        cfg.update({
            "type": "line-chart",
            "xAxis": {"column": _col(x_axis_column, "text"), "ticksLayout": ticks_layout, "formatting": None},
            "yAxis": [{"id": str(uuid.uuid4()), "column": _col(y_axis_column, "number")}],
            "metric": {"column": None if metric_column is None else _col(metric_column, "text")},
            "tooltip": {"fields": []},
            "stack": stack,
            "lineInterpolation": interpolation,
            "showLabel": show_label,
            "fallbackValue": fallback_value,
        })
        self._insight.config = cfg
        return self

    def set_pie_chart(self, label_column: SelectDef, measure_column: SelectDef,
                     appearance: str = "pie", title: str = "", subtitle: str = ""):
        cfg = self._common(title, subtitle)
        cfg.update({
            "type": "pie-chart",
            "tooltip": {"fields": []},
            "measure": {"column": _col(measure_column, "number"), "formatting": None},
            "label": {"column": _col(label_column, "text"), "formatting": None},
            "appearance": appearance,
        })
        self._insight.config = cfg
        return self

    def set_map_chart(self, geometry_column: SelectDef, label_column: Optional[SelectDef] = None,
                     value_column: Optional[SelectDef] = None, show_label: bool = True, title: str = "",
                     subtitle: str = "", layer_type: str = "choropleth", layer_title: str = "",
                     fill_style: str = "opaque", background_map: str = "osm",
                     enable_feature_grouping: Optional[bool] = None, group_column: Optional[SelectDef] = None):
        layer = {
            "type": layer_type, "fillStyle": fill_style,
            "id": str(uuid.uuid4()).replace("-", ""), "showLabel": show_label,
            "title": layer_title, "tooltip": {"fields": None},
            "geometryColumn": _col(geometry_column, "geometry"),
        }
        if label_column:
            layer["labelColumn"] = _col(label_column, "text")
        if value_column:
            layer["valueColumn"] = _col(value_column, "number")
        if layer_type == "scatter":
            layer["enableFeatureGrouping"] = bool(enable_feature_grouping)
            layer["groupColumn"] = _col(group_column, "text") if group_column else None

        cfg = self._common(title, subtitle)
        cfg.update({
            "type": "map-chart", "backgroundMap": background_map,
            "version": 2, "layers": [layer],
        })
        self._insight.config = cfg
        return self

    # --- build ------------------------------------------------------------

    def build(self) -> Dict[str, Any]:
        qb = self._insight.query.queryBuilder
        body: Dict[str, Any] = {
            "solutionId": self._insight.solutionId,
            "name": self._insight.name,
            "description": self._insight.description,
            "slug": self._insight.slug,
            "query": {
                "version": self._insight.query.version,
                "mode": self._insight.query.mode,
                "sqlEditor": {
                    "sqlString": self._insight.query.sqlEditor.sqlString,
                    "variables": [vars(v) for v in self._insight.query.sqlEditor.variables],
                },
                "queryBuilder": {
                    "version": qb.version,
                    "datasets": [vars(ds) for ds in qb.datasets],
                    "select": [vars(s) for s in qb.select],
                    "where": [{"id": w.id, "column": w.column, "operator": w.operator, "value": w.value} for w in qb.where],
                    "orderBy": [{"id": o.id, "column": o.column, "direction": o.direction} for o in qb.orderBy],
                    "pivot": {"enabled": qb.pivot.enabled, "columns": qb.pivot.columns, "rows": qb.pivot.rows, "values": qb.pivot.values},
                    "limit": qb.limit,
                },
            },
            "config": self._insight.config,
        }
        if self._insight.organizationId:
            body["organizationId"] = self._insight.organizationId
        return body
