import re
from typing import Tuple, Optional


def convert_pixel_to_percent(pixel_value: int, total_size: int) -> float:
    """Convert pixel value to percentage."""
    return round((pixel_value / total_size) * 100, 2)


def convert_map_to_relative(html_content: str, canvas_width: int = 900, canvas_height: int = 620) -> str:
    """
    Convert hardcoded pixel positions in HTML map elements to relative percentage-based positions.
    
    Args:
        html_content: The HTML string containing map elements with hardcoded pixel positions
        canvas_width: The reference width in pixels (default: 900)
        canvas_height: The reference height in pixels (default: 620)
    
    Returns:
        The modified HTML string with relative positioning
    """
    
    def convert_style_positions(match: re.Match) -> str:
        """Convert left/top pixel positions to percentages in style attributes."""
        style_content = match.group(1)
        
        # Convert left positions
        left_match = re.search(r'left:\s*(\d+)px', style_content)
        if left_match:
            left_px = int(left_match.group(1))
            left_percent = convert_pixel_to_percent(left_px, canvas_width)
            style_content = re.sub(r'left:\s*\d+px', f'left:{left_percent}%', style_content)
        
        # Convert top positions
        top_match = re.search(r'top:\s*(\d+)px', style_content)
        if top_match:
            top_px = int(top_match.group(1))
            top_percent = convert_pixel_to_percent(top_px, canvas_height)
            style_content = re.sub(r'top:\s*\d+px', f'top:{top_percent}%', style_content)
        
        # Convert width if present
        width_match = re.search(r'width:\s*(\d+)px', style_content)
        if width_match:
            width_px = int(width_match.group(1))
            width_percent = convert_pixel_to_percent(width_px, canvas_width)
            style_content = re.sub(r'width:\s*\d+px', f'width:{width_percent}%', style_content)
        
        # Convert height if present
        height_match = re.search(r'height:\s*(\d+)px', style_content)
        if height_match:
            height_px = int(height_match.group(1))
            height_percent = convert_pixel_to_percent(height_px, canvas_height)
            style_content = re.sub(r'height:\s*\d+px', f'height:{height_percent}%', style_content)
        
        return f'style="{style_content}"'
    
    def convert_canvas_dimensions(match: re.Match) -> str:
        """Convert canvas width and height attributes to percentages."""
        width_attr = match.group(1)
        height_attr = match.group(2)
        
        # Extract pixel values
        width_match = re.search(r'width="(\d+)"', width_attr)
        height_match = re.search(r'height="(\d+)"', height_attr)
        
        if width_match and height_match:
            width_px = int(width_match.group(1))
            height_px = int(height_match.group(1))
            
            width_percent = convert_pixel_to_percent(width_px, canvas_width)
            height_percent = convert_pixel_to_percent(height_px, canvas_height)
            
            # Replace with percentage-based dimensions
            new_width_attr = width_attr.replace(f'width="{width_px}"', f'width="{width_percent}%"')
            new_height_attr = height_attr.replace(f'height="{height_px}"', f'height="{height_percent}%"')
            
            return f'{new_width_attr} {new_height_attr}'
        
        return match.group(0)
    
    def convert_data_attributes(match: re.Match) -> str:
        """Convert data-pointx and data-pointy attributes to percentages."""
        element_content = match.group(0)
        
        # Convert data-pointx
        pointx_match = re.search(r'data-pointx="(\d+)"', element_content)
        if pointx_match:
            pointx_px = int(pointx_match.group(1))
            pointx_percent = convert_pixel_to_percent(pointx_px, canvas_width)
            element_content = re.sub(r'data-pointx="\d+"', f'data-pointx="{pointx_percent}"', element_content)
        
        # Convert data-pointy
        pointy_match = re.search(r'data-pointy="(\d+)"', element_content)
        if pointy_match:
            pointy_px = int(pointy_match.group(1))
            pointy_percent = convert_pixel_to_percent(pointy_px, canvas_height)
            element_content = re.sub(r'data-pointy="\d+"', f'data-pointy="{pointy_percent}"', element_content)
        
        return element_content
    
    # Convert style attributes with left/top positions
    style_pattern = r'style="([^"]*)"'
    html_content = re.sub(style_pattern, convert_style_positions, html_content)
    
    # Convert canvas width and height attributes
    canvas_pattern = r'(width="\d+")\s+(height="\d+")'
    html_content = re.sub(canvas_pattern, convert_canvas_dimensions, html_content)
    
    # Convert data-pointx and data-pointy attributes in event elements
    event_pattern = r'<div[^>]*class="[^"]*event[^"]*"[^>]*>'
    html_content = re.sub(event_pattern, convert_data_attributes, html_content)
    
    # Convert standalone left/top attributes (if any)
    left_pattern = r'left="(\d+)px"'
    html_content = re.sub(left_pattern, lambda m: f'left="{convert_pixel_to_percent(int(m.group(1)), canvas_width)}%"', html_content)
    
    top_pattern = r'top="(\d+)px"'
    html_content = re.sub(top_pattern, lambda m: f'top="{convert_pixel_to_percent(int(m.group(1)), canvas_height)}%"', html_content)
    
    return html_content


def convert_map_batch(html_list: list, canvas_width: int = 900, canvas_height: int = 620) -> list:
    """
    Convert a batch of HTML map strings to relative positioning.
    
    Args:
        html_list: List of HTML strings to convert
        canvas_width: The reference width in pixels (default: 900)
        canvas_height: The reference height in pixels (default: 620)
    
    Returns:
        List of converted HTML strings
    """
    return [convert_map_to_relative(html, canvas_width, canvas_height) for html in html_list]


def detect_canvas_dimensions(html_content: str) -> Optional[Tuple[int, int]]:
    """
    Detect the canvas dimensions from the HTML content.
    
    Args:
        html_content: The HTML string to analyze
    
    Returns:
        Tuple of (width, height) in pixels, or None if not found
    """
    # Look for canvas width and height attributes
    canvas_match = re.search(r'width="(\d+)"\s+height="(\d+)"', html_content)
    if canvas_match:
        return (int(canvas_match.group(1)), int(canvas_match.group(2)))
    
    # Look for style attributes with width and height
    style_match = re.search(r'width:(\d+)px;\s*height:(\d+)px', html_content)
    if style_match:
        return (int(style_match.group(1)), int(style_match.group(2)))
    
    return None


def convert_map_auto_detect(html_content: str) -> str:
    """
    Convert map to relative positioning with automatic canvas dimension detection.
    
    Args:
        html_content: The HTML string to convert
    
    Returns:
        The converted HTML string
    """
    dimensions = detect_canvas_dimensions(html_content)
    if dimensions:
        return convert_map_to_relative(html_content, dimensions[0], dimensions[1])
    else:
        # Fall back to default dimensions
        return convert_map_to_relative(html_content) 
