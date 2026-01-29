#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2025-2026 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
Hardware Information Parser

Parses lshw JSON output to extract hardware information into a flat structure.
Handles both VM and bare metal configurations.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Union


logger = logging.getLogger(__name__)


class HardwareInfo:
    """Parse hardware JSON files (lshw -json format) and extract key information."""

    node: str
    data: Dict[str, Any]
    input_name: str

    def __init__(self, input_name: str, raw_data: Dict[str, Any]) -> None:
        """
        Load hardware JSON file.

        Args:
                input_name: Name of the input file (for logging)
                raw_data: Parsed JSON data
        """

        if "hardware" in raw_data and isinstance(raw_data["hardware"], dict):
            hw_wrapper = raw_data["hardware"]
            self.node = hw_wrapper.get("node", "")
            self.data = hw_wrapper.get("data", {})
            self.input_name = input_name
        else:
            raise ValueError(
                f"Invalid hardware JSON format in {input_name}: missing 'hardware' wrapper"
            )

    def parse(self) -> Dict[str, Any]:
        """
        Parse hardware data and return flat structure.

        Returns:
            Flat hardware information dictionary
        """
        result = {"node": self.node}

        result.update(self._extract_system_info())
        result.update(self._extract_bios_info())
        result.update(self._extract_cpu_info())
        result.update(self._extract_memory_info())
        result["storage_devices"] = self._extract_storage_devices()
        result["network_interfaces"] = self._extract_network_interfaces()

        # Extract PCI devices by category
        pci_devices = self._extract_pci_devices()
        result["pci_storage_controllers"] = pci_devices["storage"]
        result["pci_network_controllers"] = pci_devices["network"]
        result["pci_usb_controllers"] = pci_devices["usb"]
        result["pci_accelerators"] = pci_devices["accelerator"]
        result["pci_other_devices"] = pci_devices["other"]

        return result

    def _find_nodes_by_class(
        self, node: Union[Dict[str, Any], Any], class_name: str
    ) -> List[Dict[str, Any]]:
        """
        Recursively find all nodes with given class.

        Args:
            node: Current node in tree
            class_name: Class name to search for

        Returns:
            List of matching nodes
        """
        results = []

        if not isinstance(node, dict):
            return results

        # Check current node
        if node.get("class") == class_name:
            results.append(node)

        # Recursively check children
        children = node.get("children", [])
        for child in children:
            results.extend(self._find_nodes_by_class(child, class_name))

        return results

    def _parse_vendor_string(
        self, vendor_str: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Parse vendor string to extract name and ID.

        Args:
            vendor_str: String like "Intel Corporation [8086]"

        Returns:
            (vendor_name, vendor_id) or (vendor_str, None) if no ID
        """
        if not vendor_str:
            return None, None

        match = re.match(r"^(.+?)\s*\[([0-9A-Fa-f]+)\]$", vendor_str)
        if match:
            return match.group(1).strip(), match.group(2).upper()
        return vendor_str, None

    def _parse_product_string(
        self, product_str: Optional[str]
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Parse product string to extract model and vendor:device IDs.

        Args:
            product_str: String like "NetXtreme BCM5720 [14E4:165F]"

        Returns:
            (model, vendor_id, device_id) or (product_str, None, None) if no IDs
        """
        if not product_str:
            return None, None, None

        match = re.match(r"^(.+?)\s*\[([0-9A-Fa-f]+):([0-9A-Fa-f]+)\]$", product_str)
        if match:
            return (
                match.group(1).strip(),
                match.group(2).upper(),
                match.group(3).upper(),
            )
        return product_str, None, None

    def _parse_firmware_string(
        self, firmware_str: Optional[str], vendor_name: Optional[str]
    ) -> Dict[str, Optional[str]]:
        """
        Parse composite firmware strings into primary version and extended info.

        Args:
            firmware_str: Raw firmware string from lshw
            vendor_name: Vendor name to determine parsing logic

        Returns:
            Dict with 'primary', 'bootcode', 'nvm', 'psid', etc.
        """
        if not firmware_str:
            return {"primary": None, "extended": None}

        result = {
            "primary": None,
            "extended": None,
            "bootcode": None,
            "nvm": None,
            "psid": None,
            "ncsi": None,
        }

        vendor_lower = (vendor_name or "").lower()

        # Broadcom parsing
        # Examples: "FFV21.80.8 bc 5720-v1.39", "5719-v1.55 NCSI v1.5.55.0"
        if "broadcom" in vendor_lower:
            parts = firmware_str.split()
            result["primary"] = parts[0] if parts else firmware_str

            # Look for bootcode
            if "bc" in parts:
                bc_idx = parts.index("bc")
                if bc_idx + 1 < len(parts):
                    result["bootcode"] = parts[bc_idx + 1]

            # Look for NCSI
            if "NCSI" in firmware_str:
                ncsi_match = re.search(r"NCSI\s+(v[\d.]+)", firmware_str)
                if ncsi_match:
                    result["ncsi"] = ncsi_match.group(1)

            # Extended info is everything after first part
            if len(parts) > 1:
                result["extended"] = " ".join(parts[1:])

        # Intel parsing
        # Examples: "2.33 0x80006d20 20.0.18", "1.63, 0x80001099, 1.3310.0"
        elif "intel" in vendor_lower:
            # Handle both space and comma separators
            parts = re.split(r"[,\s]+", firmware_str.strip())
            parts = [p.strip() for p in parts if p.strip()]

            result["primary"] = parts[0] if parts else firmware_str

            # Look for NVM version (last numeric part)
            if len(parts) >= 3:
                result["nvm"] = parts[-1]
                result["extended"] = f"NVM {parts[-1]}"
            elif len(parts) > 1:
                result["extended"] = " ".join(parts[1:])

        # Mellanox parsing
        # Examples: "16.28.4512 (DEL0000000015)", "14.32.2004 (HPE0000000039)"
        elif "mellanox" in vendor_lower:
            # Extract primary version before parenthesis
            match = re.match(r"^([\d.]+)\s*\(([^)]+)\)", firmware_str)
            if match:
                result["primary"] = match.group(1)
                result["psid"] = match.group(2)
                result["extended"] = f"PSID: {match.group(2)}"
            else:
                result["primary"] = firmware_str

        # Red Hat/Virtio parsing
        elif "red hat" in vendor_lower or "virtio" in vendor_lower:
            result["primary"] = firmware_str

        # Generic/Unknown vendor - use first part
        else:
            parts = firmware_str.split()
            result["primary"] = parts[0] if parts else firmware_str
            if len(parts) > 1:
                result["extended"] = " ".join(parts[1:])

        return result

    def _extract_system_info(self) -> Dict[str, Optional[str]]:
        """Extract system vendor and model from root node."""
        product = self.data.get("product")

        # Parse product string to extract clean model and SKU/part number
        model, sku = self._parse_system_model(product)

        result = {
            "system_vendor": self.data.get("vendor"),
            "system_model": model,
            "system_sku": sku,
            "system_family": None,
        }

        # Extract family from configuration if available
        config = self.data.get("configuration", {})
        if isinstance(config, dict):
            result["system_family"] = config.get("family")

        return result

    def _parse_system_model(
        self, product_str: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Parse system model string to extract clean model name and SKU/part number.

        Examples:
            "PowerEdge R750 (SKU=090E;ModelName=PowerEdge R750)"
                -> ("PowerEdge R750", "090E")
            "ProLiant DL110 Gen11 (P54277-B21)"
                -> ("ProLiant DL110 Gen11", "P54277-B21")
            "KVM (8.2.0)"
                -> ("KVM", "8.2.0")

        Args:
            product_str: Raw product string from lshw

        Returns:
            (clean_model, sku_or_version)
        """
        if not product_str:
            return None, None

        # Check if there's parenthetical data
        match = re.match(r"^(.+?)\s*\(([^)]+)\)$", product_str)
        if not match:
            return product_str, None

        base_model = match.group(1).strip()
        paren_content = match.group(2).strip()

        # Parse Dell format: "SKU=090E;ModelName=PowerEdge R750"
        if "SKU=" in paren_content:
            sku_match = re.search(r"SKU=([^;]+)", paren_content)
            if sku_match:
                sku = sku_match.group(1).strip()
                # Don't return "NotProvided" as SKU
                if sku == "NotProvided":
                    return base_model, None
                return base_model, sku

        # For other vendors (HPE, etc.), return the whole
        # parenthetical content as SKU/part number
        return base_model, paren_content

    def _is_virtual_function(self, businfo: Optional[str]) -> bool:
        """
        Detect if a PCI device is an SR-IOV Virtual Function based on bus address.

        In SR-IOV:
        - Physical Functions (PF) have device number 00 (e.g., pci@0000:9d:00.0)
          Multi-function NICs can have multiple PFs on device 00 (00.0, 00.1, 00.2, 00.3)
        - Virtual Functions (VF) have device number != 00 (e.g., pci@0000:9d:01.0)

        Args:
            businfo: PCI bus address like "pci@0000:9d:01.5"

        Returns:
            True if VF, False if PF or non-PCI device
        """
        if not businfo or not businfo.startswith("pci@"):
            return False

        # Parse PCI address: pci@domain:bus:device.function
        # Example: pci@0000:9d:01.5 -> device=01 (VF)
        #          pci@0000:9d:00.3 -> device=00 (PF, even with function=3)
        match = re.match(
            r"pci@[0-9a-fA-F]+:([0-9a-fA-F]+):([0-9a-fA-F]+)\.([0-9a-fA-F]+)", businfo
        )
        if not match:
            return False

        device_num = match.group(2)

        # VF if device number != 00
        return device_num != "00"

    def _extract_bios_info(self) -> Dict[str, Optional[str]]:
        """Extract BIOS/firmware information."""
        result = {
            "bios_vendor": None,
            "bios_version": None,
            "bios_date": None,
            "bios_type": None,
        }

        # Find the firmware node (usually has id='firmware' and class='memory')
        def find_firmware(node: Any) -> Optional[Dict[str, Any]]:
            if isinstance(node, dict):
                if node.get("id") == "firmware" and node.get("class") == "memory":
                    return node

                for child in node.get("children", []):
                    found = find_firmware(child)
                    if found:
                        return found
            return None

        firmware_node = find_firmware(self.data)

        if firmware_node:
            result["bios_vendor"] = firmware_node.get("vendor")
            result["bios_version"] = firmware_node.get("version")
            result["bios_date"] = firmware_node.get("date")

            # Determine BIOS type from description or capabilities
            description = firmware_node.get("description", "").upper()
            capabilities = firmware_node.get("capabilities", {})

            if "UEFI" in description or (
                isinstance(capabilities, dict) and "uefi" in capabilities
            ):
                result["bios_type"] = "UEFI"
            elif "EFI" in description:
                result["bios_type"] = "EFI"
            else:
                result["bios_type"] = "BIOS"

        return result

    def _extract_cpu_info(self) -> Dict[str, Any]:
        """Find all CPUs and aggregate cores/threads."""
        cpus = self._find_nodes_by_class(self.data, "processor")

        if not cpus:
            return {
                "cpu_vendor": None,
                "cpu_model": None,
                "cpu_sockets": 0,
                "cpu_total_cores": 0,
                "cpu_total_threads": 0,
                "cpu_frequency_mhz": None,
            }

        # Get vendor and model from first CPU
        first_cpu = cpus[0]
        cpu_vendor, _ = self._parse_vendor_string(first_cpu.get("vendor"))
        cpu_model = first_cpu.get("product")

        # Extract frequency (in Hz, convert to MHz)
        cpu_freq_hz = first_cpu.get("size")
        cpu_frequency_mhz = int(cpu_freq_hz / 1000000) if cpu_freq_hz else None

        # Count sockets and aggregate cores/threads
        cpu_sockets = len(cpus)
        total_cores = 0
        total_threads = 0

        for cpu in cpus:
            config = cpu.get("configuration", {})
            if isinstance(config, dict):
                cores = config.get("cores")
                threads = config.get("threads")

                # Convert to int if string
                if cores:
                    total_cores += int(cores) if isinstance(cores, str) else cores
                if threads:
                    total_threads += (
                        int(threads) if isinstance(threads, str) else threads
                    )

        return {
            "cpu_vendor": cpu_vendor,
            "cpu_model": cpu_model,
            "cpu_sockets": cpu_sockets,
            "cpu_total_cores": total_cores,
            "cpu_total_threads": total_threads,
            "cpu_frequency_mhz": cpu_frequency_mhz,
        }

    def _extract_memory_info(self) -> Dict[str, Union[float, int]]:
        """Find memory node and get total size."""
        # Look for memory node
        memory_nodes = self._find_nodes_by_class(self.data, "memory")

        total_bytes = 0
        dimm_count = 0

        for mem_node in memory_nodes:
            # Root memory node has total size
            # Match both "memory" and "memory:0", "memory:1", etc.
            node_id = mem_node.get("id", "")
            if node_id == "memory" or (
                isinstance(node_id, str) and node_id.startswith("memory:")
            ):
                # Only consider System Memory nodes (not RAM controllers, firmware, etc.)
                description = mem_node.get("description", "")
                if "System Memory" in description or node_id == "memory":
                    size = mem_node.get("size")
                    if size:
                        total_bytes += size

            # Count DIMMs (children of memory node with class memory)
            children = mem_node.get("children", [])
            for child in children:
                if child.get("class") == "memory" and child.get("size"):
                    dimm_count += 1

        total_gb = round(total_bytes / (1024**3), 1) if total_bytes else 0.0

        return {"memory_total_gb": total_gb, "memory_dimm_count": dimm_count}

    def _extract_storage_devices(self) -> List[Dict[str, Any]]:
        """Find all disks and storage devices."""
        devices = []

        # Find all disk nodes
        disk_nodes = self._find_nodes_by_class(self.data, "disk")
        # Also find storage nodes that might contain disks
        storage_nodes = self._find_nodes_by_class(self.data, "storage")

        # Process disk nodes
        for disk in disk_nodes:
            device_info = self._parse_storage_device(disk)
            if device_info:
                devices.append(device_info)

        # Process storage controllers with attached volumes
        for storage in storage_nodes:
            # Check for volume children
            children = storage.get("children", [])
            for child in children:
                if child.get("class") == "volume" or child.get("class") == "disk":
                    device_info = self._parse_storage_device(child)
                    if device_info:
                        devices.append(device_info)

        return devices

    def _parse_storage_device(self, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single storage device node."""
        # Skip if no size (not a real disk)
        size = node.get("size")
        if not size:
            return None

        # Determine device type from businfo or description
        businfo = node.get("businfo", "")
        description = node.get("description", "")

        if "nvme" in businfo.lower() or "nvme" in description.lower():
            device_type = "nvme"
        elif "scsi" in businfo.lower():
            device_type = "scsi"
        elif "virtio" in businfo.lower():
            device_type = "virtio"
        elif "sata" in businfo.lower() or "ata" in description.lower():
            device_type = "sata"
        else:
            device_type = "unknown"

        # Parse vendor and product
        vendor_str = node.get("vendor")
        product_str = node.get("product")

        vendor, vendor_id = self._parse_vendor_string(vendor_str)
        model, prod_vendor_id, device_id = self._parse_product_string(product_str)

        # Use product vendor_id if vendor_id is None
        if vendor_id is None and prod_vendor_id is not None:
            vendor_id = prod_vendor_id

        size_gb = round(size / (1024**3), 1)

        # Get version/firmware info
        version = node.get("version")
        firmware = node.get("firmware")

        # Check configuration for firmware
        config = node.get("configuration", {})
        if isinstance(config, dict) and not firmware:
            firmware = config.get("firmware")

        return {
            "type": device_type,
            "description": description,
            "vendor": vendor,
            "vendor_id": vendor_id,
            "model": model,
            "device_id": device_id,
            "size_gb": size_gb,
            "version": version,
            "firmware": firmware,
            "businfo": businfo,
        }

    def _extract_network_interfaces(self) -> List[Dict[str, Any]]:
        """Find all network devices."""
        interfaces = []

        network_nodes = self._find_nodes_by_class(self.data, "network")

        for net in network_nodes:
            interface_info = self._parse_network_interface(net)
            if interface_info:
                interfaces.append(interface_info)

        return interfaces

    def _parse_network_interface(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Parse a single network interface node."""
        description = node.get("description", "")

        # Parse vendor and product
        vendor_str = node.get("vendor")
        product_str = node.get("product")

        vendor, vendor_id = self._parse_vendor_string(vendor_str)
        model, prod_vendor_id, device_id = self._parse_product_string(product_str)

        # Use product vendor_id if vendor_id is None
        if vendor_id is None and prod_vendor_id is not None:
            vendor_id = prod_vendor_id

        # Get logical name (interface name)
        logical_name = node.get("logicalname")

        # Check children for virtio devices
        if not logical_name:
            children = node.get("children", [])
            for child in children:
                if child.get("class") == "network":
                    logical_name = child.get("logicalname")
                    if logical_name:
                        break

        # Get configuration
        config = node.get("configuration", {})

        # Get link status, duplex, and autonegotiation
        link_status = None
        duplex = None
        autonegotiation = None
        if isinstance(config, dict):
            # Convert link status from 'yes'/'no' to boolean
            link_raw = config.get("link")
            if link_raw == "yes":
                link_status = True
            elif link_raw == "no":
                link_status = False
            # else: remains None

            duplex = config.get("duplex")  # 'full', 'half', or None (keep as string)

            # Convert autonegotiation from 'on'/'off' to boolean
            autoneg_raw = config.get("autonegotiation")
            if autoneg_raw == "on":
                autonegotiation = True
            elif autoneg_raw == "off":
                autonegotiation = False
            # else: remains None

        # Get negotiated speed (actual link speed)
        speed_mbps = None
        if isinstance(config, dict):
            speed_str = config.get("speed")
            if speed_str:
                # Parse speed like "1Gbit/s" or "1000Mbit/s"
                match = re.search(r"(\d+)\s*(Gbit|Mbit)", speed_str)
                if match:
                    value = int(match.group(1))
                    unit = match.group(2)
                    speed_mbps = value * 1000 if unit == "Gbit" else value

        # Also check capabilities for speed
        if not speed_mbps:
            capabilities = node.get("capabilities", {})
            if isinstance(capabilities, dict):
                for key in capabilities:
                    if "gbit" in key.lower():
                        match = re.search(r"(\d+)gbit", key.lower())
                        if match:
                            speed_mbps = int(match.group(1)) * 1000
                            break

        # Get driver and firmware
        driver = None
        firmware_raw = None
        driver_version = None
        subvendor_id = None
        subdevice_id = None
        if isinstance(config, dict):
            driver = config.get("driver")
            firmware_raw = config.get("firmware")
            driver_version = config.get("driverversion")

            # Extract subsystem vendor/device IDs from configuration
            # These are optional and indicate the board manufacturer
            # Format: "0x1028" or "1028"
            sub_vendor = config.get("subvendor") or config.get("vendor")
            sub_device = config.get("subdevice") or config.get("device")

            if sub_vendor:
                # Strip 0x prefix if present
                subvendor_id = sub_vendor.replace("0x", "").upper()
            if sub_device:
                # Strip 0x prefix if present
                subdevice_id = sub_device.replace("0x", "").upper()

        businfo = node.get("businfo", "")

        # Parse firmware string into structured components
        firmware_parsed = self._parse_firmware_string(firmware_raw, vendor)

        # Determine if this is a Virtual Function (SR-IOV VF) by checking PCI address
        is_vf = self._is_virtual_function(businfo)

        result = {
            "description": description,
            "vendor": vendor,
            "vendor_id": vendor_id,
            "model": model,
            "device_id": device_id,
            "subvendor_id": subvendor_id,
            "subdevice_id": subdevice_id,
            "logical_name": logical_name,
            "link_status": link_status,
            "duplex": duplex,
            "autonegotiation": autonegotiation,
            "speed_mbps": speed_mbps,
            "driver": driver,
            "driver_version": driver_version,
            "firmware": firmware_raw,  # Keep original for reference
            "firmware_version": firmware_parsed.get("primary"),
            "is_virtual_function": is_vf,
            "businfo": businfo,
        }

        # Add vendor-specific firmware fields if available
        if firmware_parsed.get("bootcode"):
            result["firmware_bootcode"] = firmware_parsed["bootcode"]
        if firmware_parsed.get("nvm"):
            result["firmware_nvm"] = firmware_parsed["nvm"]
        if firmware_parsed.get("psid"):
            result["firmware_psid"] = firmware_parsed["psid"]
        if firmware_parsed.get("ncsi"):
            result["firmware_ncsi"] = firmware_parsed["ncsi"]

        return result

    def _extract_pci_devices(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract and categorize PCI devices."""
        result = {
            "storage": [],
            "network": [],
            "usb": [],
            "accelerator": [],
            "other": [],
        }

        # Find all bus nodes (PCI devices typically have class other than specific ones)
        # We'll traverse the entire tree and categorize
        self._categorize_pci_recursive(self.data, result)

        return result

    def _categorize_pci_recursive(
        self, node: Union[Dict[str, Any], Any], result: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """Recursively categorize PCI devices."""
        if not isinstance(node, dict):
            return

        businfo = node.get("businfo", "")

        # Only process PCI devices
        if businfo.startswith("pci@"):
            category = self._categorize_pci_device(node)

            if category:
                device_info = self._parse_pci_device(node)
                result[category].append(device_info)

        # Recurse to children
        children = node.get("children", [])
        for child in children:
            self._categorize_pci_recursive(child, result)

    def _categorize_pci_device(self, node: Dict[str, Any]) -> Optional[str]:
        """
        Determine PCI device category.

        Returns:
            'storage', 'network', 'usb', 'accelerator', 'other', or None to skip
        """
        node_class = node.get("class", "")
        description = node.get("description", "").lower()

        # Skip nodes we handle elsewhere (but keep network for PCI categorization)
        if node_class in ["processor", "memory", "disk", "system"]:
            return None

        # Categorize network devices
        if node_class == "network":
            return "network"

        # Categorize accelerators (5G RAN, FPGA, GPU compute)
        accelerator_keywords = [
            "accelerator",
            "processing accelerators",
            "fpga",
            "programmable logic",
            "3d controller",
            "gpu",
            "signal processing",
            "dsp",
        ]
        if any(keyword in description for keyword in accelerator_keywords):
            return "accelerator"

        # Categorize based on class and description
        if node_class == "storage":
            return "storage"
        elif "usb" in description and node_class == "bus":
            return "usb"
        elif node_class == "bridge":
            return "other"
        elif node_class == "bus":
            return "other"
        elif node_class == "display":
            return "other"
        elif node_class == "multimedia":
            return "other"
        elif node_class == "generic":
            return "other"

        return "other"

    def _parse_pci_device(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Parse a PCI device node."""
        description = node.get("description", "")

        # Parse vendor and product
        vendor_str = node.get("vendor")
        product_str = node.get("product")

        vendor, vendor_id = self._parse_vendor_string(vendor_str)
        model, prod_vendor_id, device_id = self._parse_product_string(product_str)

        # Use product vendor_id if vendor_id is None
        if vendor_id is None and prod_vendor_id is not None:
            vendor_id = prod_vendor_id

        businfo = node.get("businfo", "")
        logical_name = node.get("logicalname")

        # Extract subsystem vendor/device IDs from configuration (optional)
        config = node.get("configuration", {})
        subvendor_id = None
        subdevice_id = None
        if isinstance(config, dict):
            # Prefer subvendor/subdevice, fall back to vendor/device
            sub_vendor = config.get("subvendor") or config.get("vendor")
            sub_device = config.get("subdevice") or config.get("device")

            if sub_vendor:
                # Strip 0x prefix if present
                subvendor_id = sub_vendor.replace("0x", "").upper()
            if sub_device:
                # Strip 0x prefix if present
                subdevice_id = sub_device.replace("0x", "").upper()

        # Determine if this is a Virtual Function (SR-IOV VF) by checking PCI address
        is_vf = self._is_virtual_function(businfo)

        return {
            "description": description,
            "vendor": vendor,
            "vendor_id": vendor_id,
            "model": model,
            "device_id": device_id,
            "subvendor_id": subvendor_id,
            "subdevice_id": subdevice_id,
            "is_virtual_function": is_vf,
            "businfo": businfo,
            "logical_name": logical_name,
        }


def normalize(input_name: str, input_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize hardware data from lshw JSON format.

    Args:
        input_name: Name of the input file (for logging)
        input_data: Parsed JSON data

    Returns:
        Normalized hardware information dictionary, or None on error
    """
    try:
        normalizer = HardwareInfo(input_name, input_data)
        return normalizer.parse()
    except ValueError as e:
        logger.error(f"Error normalizing {input_name}: {e}")
        return None
