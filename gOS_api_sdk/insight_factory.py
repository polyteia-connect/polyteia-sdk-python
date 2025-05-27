import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

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
    
    def set_bar_chart(self, 
                     x_axis_column: SelectDef,
                     y_axis_column: SelectDef,
                     metric_column: Optional[SelectDef] = None,
                     bar_group_type: str = "group",
                     bar_layout: str = "vertical",
                     show_label: bool = True,
                     title: str = "",
                     subtitle: str = "",
                     ticks_layout: str = "normal") -> 'InsightBuilderV3':
        """Configure a bar chart visualization.
        
        Args:
            x_axis_column: The column for the x-axis
            y_axis_column: The column for the y-axis
            metric_column: The column used for grouping/stacking (optional)
            bar_group_type: How to group bars ("group", "stack", "stack-percentage")
            bar_layout: Bar orientation ("vertical", "horizontal")
            show_label: Whether to show labels
            title: Chart title
            subtitle: Chart subtitle
            ticks_layout: How to layout ticks ("normal", "rotate")
        """
        self._insight.config = {
            "type": "bar-chart",
            "barGroupType": bar_group_type,  # group, stack, stack-percentage
            "barLayout": bar_layout,  # vertical, horizontal
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
                      interpolation: str = "linear",
                      show_label: bool = True,
                      title: str = "",
                      subtitle: str = "") -> 'InsightBuilderV3':
        """Configure a line chart visualization."""
        self._insight.config = {
            "type": "line-chart",
            "interpolation": interpolation,  # linear, smooth, step
            "showLabel": show_label,
            "title": title,
            "subtitle": subtitle,
            "xAxis": {
                "column": {
                    "id": x_axis_column.id,
                    "key": x_axis_column.columnId,
                    "label": x_axis_column.label,
                    "type": "text"
                }
            },
            "yAxis": {
                "column": {
                    "id": y_axis_column.id,
                    "key": y_axis_column.columnId,
                    "label": y_axis_column.label,
                    "type": "number"
                }
            },
            "filters": []
        }
        return self

    def set_pie_chart(self,
                     label_column: SelectDef,
                     value_column: SelectDef,
                     appearance: str = "pie",
                     show_label: bool = True,
                     title: str = "",
                     subtitle: str = "") -> 'InsightBuilderV3':
        """Configure a pie chart visualization."""
        self._insight.config = {
            "type": "pie-chart",
            "appearance": appearance,  # pie, donut
            "showLabel": show_label,
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
            "value": {
                "column": {
                    "id": value_column.id,
                    "key": value_column.columnId,
                    "label": value_column.label,
                    "type": "number"
                }
            },
            "filters": []
        }
        return self

    def set_table(self,
                 columns: List[SelectDef],
                 show_header: bool = True,
                 title: str = "",
                 subtitle: str = "") -> 'InsightBuilderV3':
        """Configure a table visualization."""
        self._insight.config = {
            "type": "table",
            "subtitle": subtitle,
            "title": title,
            "series": [
                {
                    "column": {
                        "id": col.label,
                        "key": col.label,
                        "label": col.label,
                        "type": "number" if "%" in col.label or "anzahl" in col.label.lower() else "text"
                    },
                    "id": f"col_{i}",  # Vereinfachte ID
                    "sortable": True,
                    "title": {
                        "text": ""
                    }
                } for i, col in enumerate(columns)
            ],
            "filters": []
        }
        return self

    def set_big_number(
        self,
        measure_column: SelectDef,  # Not optional anymore
        aggregate: str = "sum",  # Default to sum like in wavy
        title: str = "",
        subtitle: str = ""
    ) -> 'InsightBuilderV3':
        """Configure a big number visualization.
        
        Args:
            measure_column: The column to measure
            aggregate: Aggregation method ('sum', 'mean', 'median', 'min', 'max')
            title: Chart title
            subtitle: Chart subtitle
        """
        # Set the config structure like in wavy
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

        # If we're in SQL mode, we still need the measure_column info
        # but the actual aggregation happens in the SQL
        if self._insight.query.mode == "sqlEditor":
            # Keep the existing SQL string
            sql = self._insight.query.sqlEditor.get("sqlString", "")
            
            # Set up the query structure
            self._insight.query = QueryDef(
                mode="sqlEditor",
                version=3,
                sqlEditor={"sqlString": sql}
            )
        else:
            # In queryBuilder mode, set up the full structure
            self._insight.query.queryBuilder = QueryBuilderDef(
                datasets=[
                    DatasetDef(
                        datasetId=measure_column.datasetId,
                        join={"type": "inner", "on": []}
                    )
                ],
                select=[measure_column],  # Use the measure_column directly
                version=2,
                limit=1000
            )

        return self

    def set_map_chart(self,
                     geometry_column: SelectDef,
                     label_column: Optional[SelectDef] = None,
                     value_column: Optional[SelectDef] = None,
                     show_label: bool = True,
                     title: str = "",
                     subtitle: str = "") -> 'InsightBuilderV3':
        """Configure a map chart visualization.
        
        Args:
            geometry_column: The column containing geometry data
            label_column: Optional column for labels
            value_column: Optional column for values (must be numeric)
            show_label: Whether to show labels
            title: Chart title
            subtitle: Chart subtitle
        """
        config = {
            "type": "map-chart",
            "title": title,
            "subtitle": subtitle,
            "showLabel": show_label,
            "geometryColumn": {
                "id": geometry_column.id,
                "key": geometry_column.columnId,
                "label": geometry_column.label,
                "type": "text"  # Geometry is stored as GeoJSON text
            },
            "filters": []
        }

        if label_column:
            config["labelColumn"] = {
                "id": label_column.id,
                "key": label_column.columnId,
                "label": label_column.label,
                "type": "text"
            }

        if value_column:
            config["valueColumn"] = {
                "id": value_column.id,
                "key": value_column.columnId,
                "label": value_column.label,
                "type": "number"
            }

        self._insight.config = config
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
