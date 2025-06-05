from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union
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

class ColumnLayout(Enum):
    TWO_EQUAL = ["50%", "50%"]
    LEFT_WIDE = ["70%", "30%"]
    RIGHT_WIDE = ["30%", "70%"]
    THREE_EQUAL = ["33.333333333333336%", "33.333333333333336%", "33.333333333333336%"]
    CENTER_WIDE = ["25%", "50%", "25%"]

    def __init__(self, widths):
        self.widths = widths

class ReportBuilder:
    def __init__(self):
        self._content = {"editorState": []}
        self._name = ""
        self._description = ""
        self._solution_id = ""
        self._organization_id = ""
        self._insights = []  # Track insights for metadata
        self._current_column_group = None
        self._current_column = None

    def _generate_block_id(self) -> str:
        """Generate a unique ID for a block"""
        return str(uuid.uuid4()).replace('-', '')[:10]

    def set_name(self, name: str) -> 'ReportBuilder':
        """Set the report name"""
        self._name = name
        return self

    def set_description(self, description: str) -> 'ReportBuilder':
        """Set the report description"""
        self._description = description
        return self

    def set_solution_id(self, solution_id: str) -> 'ReportBuilder':
        """Set the solution ID"""
        self._solution_id = solution_id
        return self

    def set_organization_id(self, organization_id: str) -> 'ReportBuilder':
        """Set the organization ID"""
        self._organization_id = organization_id
        return self

    def add_heading(self, text: str, level: HeadingLevel = HeadingLevel.H1, align: Optional[TextAlign] = None) -> 'ReportBuilder':
        """Add a heading to the report
        
        Args:
            text: The heading text
            level: Heading level (H1-H6)
            align: Text alignment
        """
        block = {
            "type": level.value,
            "id": self._generate_block_id(),
            "children": [{"text": text}]
        }
        if align:
            block["align"] = align.value
        self._add_block(block)
        return self

    def add_text(self, text: str, formatting: Optional[TextFormatting] = None, align: Optional[TextAlign] = None) -> 'ReportBuilder':
        """Add text to the report
        
        Args:
            text: The text content
            formatting: Text formatting options
            align: Text alignment
        """
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
            
        self._add_block(block)
        return self

    def add_list(self, items: List[str], list_type: ListType = ListType.BULLET) -> 'ReportBuilder':
        """Add a list to the report
        
        Args:
            items: List items
            list_type: Type of list (bullet, numbered, todo)
        """
        self._add_block({
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
        """Add a table to the report
        
        Args:
            rows: Table rows (first row is header if has_header=True)
            has_header: Whether first row is a header
        """
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
        
        self._add_block({
            "type": "table",
            "id": self._generate_block_id(),
            "children": table_rows
        })
        return self

    def add_widget(self, insight_id: str, height: Optional[int] = None) -> 'ReportBuilder':
        """Add an insight widget to the report
        
        Args:
            insight_id: ID of the insight to add
        """
        # Track insight for metadata
        if insight_id not in self._insights:
            self._insights.append(insight_id)

        widget = {
            "type": "widget",
            "id": self._generate_block_id(),
            "children": [{"text": ""}],
            "widgetData": {
                "insightId": insight_id
            },
            "height": height
        }

        if self._current_column_group and self._current_column is not None:
            self._current_column_group["children"][self._current_column]["children"].append(widget)
        else:
            self._content["editorState"].append(widget)
        return self

    def add_horizontal_rule(self) -> 'ReportBuilder':
        """Add a horizontal rule to the report"""
        self._add_block({
            "type": "hr",
            "id": self._generate_block_id(),
            "children": [{"text": ""}]
        })
        return self

    def add_link(self, text: str, url: str) -> 'ReportBuilder':
        """Add a link to the report
        
        Args:
            text: Link text
            url: Link URL
        """
        self._add_block({
            "type": "link",
            "id": self._generate_block_id(),
            "url": url,
            "children": [{"text": text}]
        })
        return self

    def add_blockquote(self, text: str) -> 'ReportBuilder':
        """Add a blockquote to the report
        
        Args:
            text: The text to quote
        """
        self._add_block({
            "type": "blockquote",
            "id": self._generate_block_id(),
            "children": [{"text": text}]
        })
        return self

    def add_code(self, code: str, language: Optional[str] = None) -> 'ReportBuilder':
        """Add a code block to the report
        
        Args:
            code: The code to display
            language: Programming language for syntax highlighting
        """
        self._add_block({
            "type": "code_block",
            "id": self._generate_block_id(),
            "language": language,
            "children": [{"type": "code_line", "children": [{"text": code}]}]
        })
        return self

    def add_date(self, date: str) -> 'ReportBuilder':
        """Add a date to the report
        
        Args:
            date: The date to display
        """
        self._add_block({
            "type": "date",
            "id": self._generate_block_id(),
            "children": [{"text": date}]
        })
        return self

    def add_toggle(self, header: str, content: Union[str, List[Dict[str, Any]]]) -> 'ReportBuilder':
        """Add a collapsible toggle section to the report
        
        Args:
            header: The toggle header text
            content: The content to show/hide (string or rich text)
        """
        if isinstance(content, str):
            content = [{"text": content}]

        self._add_block({
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
        """Add a mathematical equation to the report
        
        Args:
            equation: The equation in LaTeX format
            inline: Whether to display inline or as a block
        """
        self._add_block({
            "type": "equation" if not inline else "inline_equation",
            "id": self._generate_block_id(),
            "equation": equation,
            "children": [{"text": ""}]
        })
        return self

    def add_dataset(self, dataset_id: str) -> 'ReportBuilder':
        """Explicitly add a dataset to the report metadata
        
        Args:
            dataset_id: ID of the dataset to add
        """
        if dataset_id not in self._metadata["datasets"]:
            self._metadata["datasets"].append(dataset_id)
        return self

    def start_columns(self, layout: Union[ColumnLayout, List[str]]) -> 'ReportBuilder':
        """Start a new column group with the specified layout.
        
        Args:
            layout: Either a ColumnLayout enum value or a list of width strings (e.g. ["70%", "30%"])
        """
        widths = layout.widths if isinstance(layout, ColumnLayout) else layout
        
        self._current_column_group = {
            "type": "column_group",
            "id": self._generate_block_id(),
            "children": []
        }
        
        for width in widths:
            column = {
                "type": "column",
                "id": self._generate_block_id(),
                "width": width,
                "children": []
            }
            self._current_column_group["children"].append(column)
        
        self._current_column = 0
        return self

    def next_column(self) -> 'ReportBuilder':
        """Move to the next column in the current column group"""
        if not self._current_column_group:
            raise Exception("No active column group")
            
        self._current_column += 1
        if self._current_column >= len(self._current_column_group["children"]):
            raise Exception("No more columns available in this group")
            
        return self

    def end_columns(self) -> 'ReportBuilder':
        """End the current column group."""
        if self._current_column_group:
            # Add empty paragraph only to completely empty columns
            for column in self._current_column_group["children"]:
                if not column["children"]:
                    column["children"].append({
                        "type": "p",
                        "id": self._generate_block_id(),
                        "children": [{"text": ""}]
                    })
            
            self._content["editorState"].append(self._current_column_group)
            self._current_column_group = None
            self._current_column = None
        return self

    def _add_block(self, block: Dict[str, Any]):
        """Add a block to the current column or main content"""
        if self._current_column_group and self._current_column is not None:
            # Wrap block in a children array if it's going into a column
            if "children" not in block:
                block = {"children": [block]}
            self._current_column_group["children"][self._current_column]["children"].append(block)
        else:
            self._content["editorState"].append(block)

    def build(self) -> Dict[str, Any]:
        """Build the final report structure"""
        if self._current_column_group:
            self._content["editorState"].append(self._current_column_group)
            self._current_column_group = None
            self._current_column = None

        return {
            "organization_id": self._organization_id,
            "solution_id": self._solution_id,
            "name": self._name,
            "description": self._description,
            "content": self._content,
            "metadata": {
                "insights": self._insights
            }
        } 