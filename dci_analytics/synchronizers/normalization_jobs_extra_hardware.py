#!/usr/bin/env python
"""
Normalize DCI lshw JSON files for consistent type handling in ElasticSearch.

This script processes DCI format files: {"hardware": {"node": "...", "data": {...}}}

It performs:
1. Validates files are in DCI lshw format (hardware.data has 'id' and 'class' fields)
2. Converts numeric strings to proper numbers
3. Converts boolean strings to proper booleans
4. Ensures consistent types for the same fields
5. Handles logicalname field (can be string or array, normalizes to always array)
6. Optionally copies original files to output directory (removing 'dci-extra.' prefix)
7. Skips invalid files (not real DCI lshw output) and reports them
"""

import json
import logging
from typing import Any, Dict, List, Union


logger = logging.getLogger(__name__)


class LshwNormalizer:
    def __init__(self):
        # Define fields that should always be numeric (based on analysis)
        self.numeric_fields = {
            "latency",
            "cores",
            "enabledcores",
            "microcode",
            "threads",
            "level",
            "ansiversion",
            "size",
            "capacity",
            "width",
            "clock",
            "units",
            "depth",
            "FATs",
            "logicalsectorsize",
            "sectorsize",
        }

        # Define fields that should always be boolean (based on analysis)
        self.boolean_fields = {
            "claimed",
            "disabled",
            "boot",
            "broadcast",
            "link",
            "multicast",
            "slave",
            "removable",
            "audio",
            "dvd",
        }

        # Fields in capabilities that are typically boolean
        self.capability_boolean_patterns = [
            "pci",
            "pciexpress",
            "pm",
            "msi",
            "msix",
            "bus_master",
            "cap_list",
            "rom",
            "fb",
            "pnp",
            "upgrade",
            "shadowing",
            "cdboot",
            "bootselect",
            "edd",
            "usb",
            "netboot",
            "acpi",
            "biosbootspecification",
            "uefi",
            "escd",
            "virtualmachine",
            "smp",
            "vsyscall32",
            "gpt-1_00",
            "partitioned",
            "partitioned:gpt",
            "nofs",
            "fat",
            "initialized",
            "journaled",
            "extended_attributes",
            "large_files",
            "huge_files",
            "dir_nlink",
            "recover",
            "extents",
            "ethernet",
            "physical",
            "removable",
            "audio",
            "dvd",  # Media capabilities
        ]

    def is_valid_lshw(self, data: Any) -> bool:
        """
        Check if the JSON data is a valid DCI lshw output.

        Valid format: {"hardware": {"node": "...", "data": {...lshw output...}}}
        The lshw output inside must have "id" and "class" fields.

        Returns:
            True if valid DCI lshw output, False otherwise
        """
        if not isinstance(data, dict):
            return False

        # Check for DCI wrapped format
        if "hardware" not in data:
            return False

        hardware = data["hardware"]
        if not isinstance(hardware, dict):
            return False

        if "data" not in hardware:
            return False

        lshw_data = hardware["data"]
        if not isinstance(lshw_data, dict):
            return False

        # Check that the lshw data has required fields
        return "id" in lshw_data and "class" in lshw_data

    def normalize_boolean(self, value: Any) -> bool:
        """Convert various boolean representations to actual boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lower_val = value.lower().strip()
            if lower_val in ("true", "yes", "1", "on"):
                return True
            elif lower_val in ("false", "no", "0", "off"):
                return False
        # If it's a number, treat 0 as False, anything else as True
        if isinstance(value, (int, float)):
            return value != 0
        # Return as-is if we can't convert
        return value

    def normalize_numeric(
        self, value: Any, field_name: str = ""
    ) -> Union[int, float, str]:
        """Convert numeric strings to numbers."""
        if isinstance(value, (int, float)):
            return value

        if isinstance(value, str):
            # Try to convert to int first
            try:
                result = int(value)
                return result
            except (ValueError, TypeError):
                pass

            # Try to convert to float
            try:
                result = float(value)
                return result
            except (ValueError, TypeError):
                pass

        # Return as-is if we can't convert
        return value

    def normalize_logicalname(self, value: Any) -> List[str]:
        """Normalize logicalname to always be an array."""
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [value]
        return value

    def normalize_node(self, node: Any, path: str = "root") -> Any:
        """Recursively normalize a node in the JSON structure."""
        if isinstance(node, dict):
            normalized = {}
            for key, value in node.items():
                field_path = f"{path}.{key}"

                # Handle configuration fields
                if key == "configuration" and isinstance(value, dict):
                    normalized[key] = self.normalize_configuration(value, field_path)
                # Handle capabilities fields (often have boolean values)
                elif key == "capabilities" and isinstance(value, dict):
                    normalized[key] = self.normalize_capabilities(value, field_path)
                # Handle logicalname (can be string or array, normalize to array)
                elif key == "logicalname":
                    normalized[key] = self.normalize_logicalname(value)
                # Handle physid (sometimes numeric string, keep as string for consistency)
                elif key == "physid":
                    # Keep as string for consistency
                    normalized[key] = str(value) if value is not None else value
                # Handle version (can be numeric or string, keep as string for consistency)
                elif key == "version":
                    # Keep as string for consistency
                    normalized[key] = str(value) if value is not None else value
                # Handle known boolean fields
                elif key in self.boolean_fields:
                    normalized[key] = self.normalize_boolean(value)
                # Handle known numeric fields
                elif key in self.numeric_fields:
                    normalized[key] = self.normalize_numeric(value, key)
                # Recursively handle nested objects and arrays
                elif isinstance(value, dict):
                    normalized[key] = self.normalize_node(value, field_path)
                elif isinstance(value, list):
                    normalized[key] = [
                        (
                            self.normalize_node(item, f"{field_path}[{i}]")
                            if isinstance(item, (dict, list))
                            else item
                        )
                        for i, item in enumerate(value)
                    ]
                else:
                    normalized[key] = value

            return normalized

        elif isinstance(node, list):
            return [
                (
                    self.normalize_node(item, f"{path}[{i}]")
                    if isinstance(item, (dict, list))
                    else item
                )
                for i, item in enumerate(node)
            ]

        return node

    def normalize_configuration(
        self, config: Dict[str, Any], path: str
    ) -> Dict[str, Any]:
        """Normalize configuration object."""
        normalized = {}
        for key, value in config.items():
            # Boolean configuration fields
            if key in self.boolean_fields:
                normalized[key] = self.normalize_boolean(value)
            # Numeric configuration fields
            elif key in self.numeric_fields:
                normalized[key] = self.normalize_numeric(value, key)
            # Keep other fields as-is
            else:
                normalized[key] = value

        return normalized

    def normalize_capabilities(
        self, capabilities: Dict[str, Any], path: str
    ) -> Dict[str, Any]:
        """Normalize capabilities object."""
        normalized = {}
        for key, value in capabilities.items():
            # Check if this capability should be boolean
            if key in self.capability_boolean_patterns or isinstance(value, bool):
                # If value is currently a string, try to normalize to boolean
                if isinstance(value, str):
                    lower_val = value.lower().strip()
                    # Check for explicit yes/no/true/false
                    if lower_val in ("true", "false", "yes", "no", "1", "0"):
                        normalized[key] = self.normalize_boolean(value)
                    else:
                        # For descriptive strings, check for negative indicators
                        # If it contains "no", "not", "none", "disabled", "unsupported" -> False
                        # Otherwise, presence of descriptive text means capability exists -> True
                        negative_words = [
                            " no ",
                            "not ",
                            "none",
                            "disabled",
                            "unsupported",
                            "unavailable",
                        ]
                        if any(neg in lower_val for neg in negative_words):
                            normalized[key] = False
                        else:
                            # Descriptive string means capability is present
                            normalized[key] = True
                else:
                    # Already a boolean or other type
                    normalized[key] = value
            else:
                normalized[key] = value

        return normalized

    def normalize(self, input_name: str, input_data: Dict) -> Dict:
        """
        Normalize a single lshw JSON data.

        Args:
            input_name: name of the data
            input_data: the actual data
            output_path: Path to output file (if None, overwrites input)

        Returns:
            Normalized Dict if succeeded, otherwise an empty Dict
        """
        try:

            # Validate that this is a real lshw output
            if not self.is_valid_lshw(input_data):
                skip_msg = f"Skipping {input_name}: Not a valid lshw output (missing 'id' or 'class' fields)"
                logger.info(skip_msg)
                return dict()

            # Normalize the DCI wrapped format
            # Extract and normalize the lshw data inside hardware.data
            normalized_lshw = self.normalize_node(input_data["hardware"]["data"])

            # Reconstruct the DCI wrapper
            return {
                "hardware": {
                    "node": input_data["hardware"].get("node"),
                    "data": normalized_lshw,
                    "error": input_data["hardware"].get("error", ""),
                }
            }
        except json.JSONDecodeError as e:
            error_msg = f"Error parsing {input_name}: {e}"
            logger.error(error_msg)
            return dict()

        except Exception as e:
            error_msg = f"Error processing {input_name}: {e}"
            logger.error(error_msg)
            return dict()


def normalize(input_name, input_data):
    normalizer = LshwNormalizer()
    return normalizer.normalize(input_name, input_data)
