from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import re
import uuid

class HeadingLevel(Enum):
    H1 = "h1"
    H2 = "h2"
    H3 = "h3"
    H4 = "h4"
    H5 = "h5"
    H6 = "h6"

class ListType(Enum):
    BULLET = "disc"
    NUMBERED = "decimal"
    TODO = "todo"

class TextAlign(Enum):
    LEFT = "left"
    CENTER = "center"
    RIGHT = "right"
    JUSTIFY = "justify"

@dataclass
class TextFormatting:
    bold: bool = False
    italic: bool = False
    underline: bool = False
    strikethrough: bool = False
    code: bool = False
    highlight: bool = False
    color: Optional[str] = None
    background_color: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in vars(self).items() if v}

class ReportBuilder:
    def __init__(self):
        self._content = {
            "editorState": []
        }
        self._name = ""
        self._description = ""
        self._solution_id = ""
        self._organization_id = ""
        self._metadata = {
            "datasets": []
        }

    def _generate_block_id(self) -> str:
        """Generate a unique ID for a block"""
        return str(uuid.uuid4()).replace('-', '')[:10]

    def set_name(self, name: str) -> 'ReportBuilder':
        self._name = name
        return self

    def set_description(self, description: str) -> 'ReportBuilder':
        self._description = description
        return self

    def set_solution_id(self, solution_id: str) -> 'ReportBuilder':
        self._solution_id = solution_id
        return self

    def set_organization_id(self, organization_id: str) -> 'ReportBuilder':
        self._organization_id = organization_id
        return self

    def add_heading(self, text: str, level: HeadingLevel = HeadingLevel.H1, align: Optional[TextAlign] = None) -> 'ReportBuilder':
        block = {
            "type": level.value,
            "id": self._generate_block_id(),
            "children": [{"text": text}]
        }
        if align:
            block["align"] = align.value
        self._content["editorState"].append(block)
        return self

    def add_text(self, text: str, formatting: Optional[TextFormatting] = None, align: Optional[TextAlign] = None) -> 'ReportBuilder':
        text_block = {"text": text}
        if formatting:
            text_block.update(formatting.to_dict())
            
        block = {
            "type": "p",
            "id": self._generate_block_id(),
            "children": [text_block]
        }
        if align:
            block["align"] = align.value
            
        self._content["editorState"].append(block)
        return self

    def add_list(self, items: List[str], list_type: ListType = ListType.BULLET) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "list",
            "id": self._generate_block_id(),
            "listStyleType": list_type.value,
            "children": [
                {"type": "li", "children": [{"text": item}]}
                for item in items
            ]
        })
        return self

    def add_table(self, rows: List[List[str]], has_header: bool = True) -> 'ReportBuilder':
        table_rows = []
        for i, row in enumerate(rows):
            cells = []
            cell_type = "th" if i == 0 and has_header else "td"
            for cell in row:
                cells.append({
                    "type": cell_type,
                    "children": [{"text": cell}]
                })
            table_rows.append({
                "type": "tr",
                "children": cells
            })
        
        self._content["editorState"].append({
            "type": "table",
            "id": self._generate_block_id(),
            "children": table_rows
        })
        return self

    def add_blockquote(self, text: str) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "blockquote",
            "id": self._generate_block_id(),
            "children": [{"text": text}]
        })
        return self

    def add_code(self, code: str, language: Optional[str] = None) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "code_block",
            "id": self._generate_block_id(),
            "language": language,
            "children": [{"type": "code_line", "children": [{"text": code}]}]
        })
        return self

    def add_widget(self, insight_id: str, insight_data: Dict[str, Any], height: int = 400) -> 'ReportBuilder':
        """Add a widget using provided insight data"""
        # Extract data from insight response
        insight = insight_data.get('data', insight_data)  # Handle both raw and API response format
        
        # Extract datasets from query to maintain metadata
        query = insight.get("query", {})
        if "queryBuilder" in query:
            for ds in query["queryBuilder"].get("datasets", []):
                if "datasetId" in ds and ds["datasetId"] not in self._metadata["datasets"]:
                    self._metadata["datasets"].append(ds["datasetId"])
        elif "sqlEditor" in query:
            sql = query["sqlEditor"].get("sqlString", "")
            for match in re.findall(r"\{\{(ds_[\w]+)\}\}", sql):
                if match not in self._metadata["datasets"]:
                    self._metadata["datasets"].append(match)

        # Create widget data with correct structure
        widget_data = {
            "id": insight.get("id"),
            "organization_id": insight.get("organization_id"),
            "solution_id": insight.get("solution_id"),
            "name": insight.get("name"),
            "description": insight.get("description"),
            "query": insight.get("query", {}),
            "vizState": insight.get("config", {}),  # Use config as vizState
            "created_at": insight.get("created_at"),
            "updated_at": insight.get("updated_at"),
            "height": height
        }
            
        self._content["editorState"].append({
            "type": "widget",
            "id": self._generate_block_id(),
            "children": [{"text": ""}],
            "widgetData": widget_data
        })

        return self

    def add_columns(self, columns: List[List[Dict[str, Any]]]) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "column_group",
            "id": self._generate_block_id(),
            "children": [
                {"type": "column", "children": col}
                for col in columns
            ]
        })
        return self

    def add_horizontal_rule(self) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "hr",
            "id": self._generate_block_id(),
            "children": [{"text": ""}]
        })
        return self

    def add_link(self, text: str, url: str) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "link",
            "id": self._generate_block_id(),
            "url": url,
            "children": [{"text": text}]
        })
        return self

    def add_toggle(self, header: str, content: Union[str, List[Dict[str, Any]]]) -> 'ReportBuilder':
        if isinstance(content, str):
            content = [{"text": content}]

        self._content["editorState"].append({
            "type": "toggle",
            "id": self._generate_block_id(),
            "children": [
                {"text": header},
                {
                    "type": "toggle_content",
                    "children": content
                }
            ]
        })
        return self

    def add_equation(self, equation: str, inline: bool = False) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "equation" if not inline else "inline_equation",
            "id": self._generate_block_id(),
            "equation": equation,
            "children": [{"text": ""}]
        })
        return self

    def add_date(self, date: str) -> 'ReportBuilder':
        self._content["editorState"].append({
            "type": "date",
            "id": self._generate_block_id(),
            "children": [{"text": date}]
        })
        return self

    def add_dataset(self, dataset_id: str) -> 'ReportBuilder':
        """Explicitly add a dataset to the report metadata"""
        if dataset_id not in self._metadata["datasets"]:
            self._metadata["datasets"].append(dataset_id)
        return self

    def build(self) -> Dict[str, Any]:
        return {
            "organization_id": self._organization_id,
            "solution_id": self._solution_id,
            "name": self._name,
            "description": self._description,
            "content": self._content,
            "metadata": self._metadata
        } 