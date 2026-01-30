#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 Red Hat, Inc
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

"""Unit tests for normalization_jobs_extra_hardware module."""

import pytest

from dci_analytics.synchronizers import normalization_jobs_extra_hardware as hw


# Anonymized VM sample data (based on KVM/QEMU virtual machine)
VM_HARDWARE_SAMPLE = {
    "hardware": {
        "node": "test-vm-worker-1",
        "data": {
            "id": "computer",
            "class": "system",
            "description": "Computer",
            "product": "KVM (8.6.0)",
            "vendor": "Red Hat",
            "version": "RHEL-8.6.0 PC (Q35 + ICH9, 2009)",
            "configuration": {
                "boot": "normal",
                "family": "Red Hat Enterprise Linux",
                "sku": "8.6.0",
            },
            "children": [
                {
                    "id": "core",
                    "class": "bus",
                    "description": "Motherboard",
                    "product": "RHEL-AV",
                    "vendor": "Red Hat",
                    "children": [
                        {
                            "id": "cpu:0",
                            "class": "processor",
                            "description": "CPU",
                            "product": "Intel(R) Xeon(R) Gold 6330N CPU @ 2.20GHz",
                            "vendor": "Intel Corp.",
                            "businfo": "cpu@0",
                            "size": 2200000000,
                            "configuration": {
                                "cores": "4",
                                "enabledcores": "4",
                                "threads": "8",
                            },
                        },
                        {
                            "id": "cpu:1",
                            "class": "processor",
                            "description": "CPU",
                            "product": "Intel(R) Xeon(R) Gold 6330N CPU @ 2.20GHz",
                            "vendor": "Intel Corp.",
                            "businfo": "cpu@1",
                            "size": 2200000000,
                            "configuration": {
                                "cores": "4",
                                "enabledcores": "4",
                                "threads": "8",
                            },
                        },
                        {
                            "id": "memory",
                            "class": "memory",
                            "description": "System Memory",
                            "size": 68719476736,  # 64 GB
                            "children": [
                                {
                                    "id": "bank:0",
                                    "class": "memory",
                                    "description": "DIMM RAM",
                                    "vendor": "Red Hat",
                                    "size": 17179869184,
                                },
                                {
                                    "id": "bank:1",
                                    "class": "memory",
                                    "description": "DIMM RAM",
                                    "vendor": "Red Hat",
                                    "size": 17179869184,
                                },
                                {
                                    "id": "bank:2",
                                    "class": "memory",
                                    "description": "DIMM RAM",
                                    "vendor": "Red Hat",
                                    "size": 17179869184,
                                },
                                {
                                    "id": "bank:3",
                                    "class": "memory",
                                    "description": "DIMM RAM",
                                    "vendor": "Red Hat",
                                    "size": 17179869184,
                                },
                            ],
                        },
                        {
                            "id": "firmware",
                            "class": "memory",
                            "description": "BIOS",
                            "vendor": "EFI Development Kit II / OVMF",
                            "version": "0.0.0",
                            "date": "02/06/2015",
                            "capabilities": {"uefi": "UEFI specification is supported"},
                        },
                        {
                            "id": "pci",
                            "class": "bridge",
                            "description": "Host bridge",
                            "product": "82G33/G31/P35/P31 Express DRAM Controller [8086:29C0]",
                            "vendor": "Intel Corporation [8086]",
                            "businfo": "pci@0000:00:00.0",
                            "children": [
                                {
                                    "id": "network",
                                    "class": "network",
                                    "description": "Ethernet controller",
                                    "product": "Virtio 1.0 network device [1AF4:1041]",
                                    "vendor": "Red Hat, Inc. [1AF4]",
                                    "businfo": "pci@0000:03:00.0",
                                    "configuration": {
                                        "driver": "virtio-pci",
                                    },
                                    "children": [
                                        {
                                            "id": "virtio0",
                                            "class": "network",
                                            "description": "Ethernet interface",
                                            "logicalname": "enp3s0",
                                            "configuration": {
                                                "autonegotiation": "off",
                                                "driver": "virtio_net",
                                                "driverversion": "1.0.0",
                                                "link": "yes",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "id": "scsi",
                                    "class": "storage",
                                    "description": "SCSI storage controller",
                                    "product": "Virtio 1.0 SCSI [1AF4:1048]",
                                    "vendor": "Red Hat, Inc. [1AF4]",
                                    "businfo": "pci@0000:05:00.0",
                                    "children": [
                                        {
                                            "id": "virtio2",
                                            "class": "generic",
                                            "description": "Virtual I/O device",
                                            "children": [
                                                {
                                                    "id": "disk:0",
                                                    "class": "disk",
                                                    "description": "SCSI Disk",
                                                    "product": "QEMU HARDDISK",
                                                    "vendor": "QEMU",
                                                    "businfo": "scsi@0:0.0.0",
                                                    "logicalname": "/dev/sda",
                                                    "size": 107374182400,
                                                }
                                            ],
                                        }
                                    ],
                                },
                            ],
                        },
                    ],
                }
            ],
        },
    }
}


# Anonymized bare metal sample data (based on Dell PowerEdge server)
BARE_METAL_HARDWARE_SAMPLE = {
    "hardware": {
        "node": "server-sno-01",
        "data": {
            "id": "computer",
            "class": "system",
            "description": "Rack Mount Chassis",
            "product": "PowerEdge R750 (SKU=090E;ModelName=PowerEdge R750)",
            "vendor": "Dell Inc.",
            "configuration": {"family": "PowerEdge"},
            "children": [
                {
                    "id": "core",
                    "class": "bus",
                    "description": "Motherboard",
                    "product": "0K3GYT",
                    "vendor": "Dell Inc.",
                    "children": [
                        {
                            "id": "cpu:0",
                            "class": "processor",
                            "description": "CPU",
                            "product": "Intel(R) Xeon(R) Gold 6338N CPU @ 2.20GHz",
                            "vendor": "Intel Corp. [8086]",
                            "businfo": "cpu@0",
                            "size": 2200000000,
                            "configuration": {
                                "cores": "32",
                                "enabledcores": "32",
                                "threads": "64",
                            },
                        },
                        {
                            "id": "cpu:1",
                            "class": "processor",
                            "description": "CPU",
                            "product": "Intel(R) Xeon(R) Gold 6338N CPU @ 2.20GHz",
                            "vendor": "Intel Corp. [8086]",
                            "businfo": "cpu@1",
                            "size": 2200000000,
                            "configuration": {
                                "cores": "32",
                                "enabledcores": "32",
                                "threads": "64",
                            },
                        },
                        {
                            "id": "memory:0",
                            "class": "memory",
                            "description": "System Memory",
                            "size": 274877906944,  # 256 GB
                            "children": [
                                {
                                    "id": "bank:0",
                                    "class": "memory",
                                    "description": "DIMM DDR4",
                                    "vendor": "Samsung",
                                    "size": 34359738368,
                                },
                                {
                                    "id": "bank:1",
                                    "class": "memory",
                                    "description": "DIMM DDR4",
                                    "vendor": "Samsung",
                                    "size": 34359738368,
                                },
                            ],
                        },
                        {
                            "id": "memory:1",
                            "class": "memory",
                            "description": "System Memory",
                            "size": 274877906944,  # 256 GB
                            "children": [
                                {
                                    "id": "bank:0",
                                    "class": "memory",
                                    "description": "DIMM DDR4",
                                    "vendor": "Samsung",
                                    "size": 34359738368,
                                },
                                {
                                    "id": "bank:1",
                                    "class": "memory",
                                    "description": "DIMM DDR4",
                                    "vendor": "Samsung",
                                    "size": 34359738368,
                                },
                            ],
                        },
                        {
                            "id": "firmware",
                            "class": "memory",
                            "description": "BIOS",
                            "vendor": "Dell Inc.",
                            "version": "2.9.1",
                            "date": "01/15/2024",
                            "capabilities": {"uefi": "UEFI specification is supported"},
                        },
                        {
                            "id": "pci:0",
                            "class": "bridge",
                            "description": "Host bridge",
                            "product": "Intel Corporation",
                            "vendor": "Intel Corporation [8086]",
                            "businfo": "pci@0000:00:00.0",
                            "children": [
                                {
                                    "id": "network:0",
                                    "class": "network",
                                    "description": "Ethernet interface",
                                    "product": "NetXtreme BCM5720 [14E4:165F]",
                                    "vendor": "Broadcom Inc. [14E4]",
                                    "businfo": "pci@0000:04:00.0",
                                    "logicalname": "eno8303",
                                    "configuration": {
                                        "autonegotiation": "on",
                                        "driver": "tg3",
                                        "driverversion": "3.137",
                                        "firmware": "FFV21.80.8 bc 5720-v1.39",
                                        "link": "no",
                                        "speed": "1Gbit/s",
                                        "duplex": "full",
                                    },
                                },
                                {
                                    "id": "network:1",
                                    "class": "network",
                                    "description": "Ethernet interface",
                                    "product": "Ethernet Controller E810-XXV [8086:159B]",
                                    "vendor": "Intel Corporation [8086]",
                                    "businfo": "pci@0000:51:00.0",
                                    "logicalname": "ens1f0",
                                    "configuration": {
                                        "autonegotiation": "on",
                                        "driver": "ice",
                                        "driverversion": "1.9.11",
                                        "firmware": "4.20 0x8001778b 22.0.9",
                                        "link": "yes",
                                        "speed": "25Gbit/s",
                                        "duplex": "full",
                                    },
                                },
                                {
                                    "id": "network:2",
                                    "class": "network",
                                    "description": "Ethernet interface",
                                    "product": "MT27710 Family [ConnectX-4 Lx] [15B3:1015]",
                                    "vendor": "Mellanox Technologies [15B3]",
                                    "businfo": "pci@0000:9d:00.0",
                                    "logicalname": "ens5f0",
                                    "configuration": {
                                        "autonegotiation": "on",
                                        "driver": "mlx5_core",
                                        "driverversion": "5.7-1.0.2",
                                        "firmware": "14.32.2004 (DEL0000000015)",
                                        "link": "yes",
                                        "speed": "25Gbit/s",
                                        "duplex": "full",
                                    },
                                },
                                # SR-IOV Virtual Function example
                                {
                                    "id": "network:3",
                                    "class": "network",
                                    "description": "Ethernet interface",
                                    "product": "MT27710 Family [ConnectX-4 Lx Virtual Function] [15B3:1016]",
                                    "vendor": "Mellanox Technologies [15B3]",
                                    "businfo": "pci@0000:9d:01.0",  # VF - device != 00
                                    "logicalname": "ens5f0v0",
                                    "configuration": {
                                        "driver": "mlx5_core",
                                        "link": "yes",
                                    },
                                },
                                {
                                    "id": "storage",
                                    "class": "storage",
                                    "description": "RAID bus controller",
                                    "product": "PERC H755 Controller [1028:2270]",
                                    "vendor": "Dell [1028]",
                                    "businfo": "pci@0000:65:00.0",
                                },
                                {
                                    "id": "nvme",
                                    "class": "storage",
                                    "description": "Non-Volatile memory controller",
                                    "product": "NVMe SSD Controller PM1733 [144D:A824]",
                                    "vendor": "Samsung Electronics Co Ltd [144D]",
                                    "businfo": "pci@0000:c1:00.0",
                                    "children": [
                                        {
                                            "id": "namespace:0",
                                            "class": "disk",
                                            "description": "NVMe disk",
                                            "businfo": "nvme@0:1",
                                            "logicalname": "/dev/nvme0n1",
                                            "size": 1920383410176,
                                        }
                                    ],
                                },
                                {
                                    "id": "generic",
                                    "class": "generic",
                                    "description": "Processing accelerators",
                                    "product": "ACC100 [8086:0D5C]",
                                    "vendor": "Intel Corporation [8086]",
                                    "businfo": "pci@0000:b1:00.0",
                                },
                            ],
                        },
                    ],
                }
            ],
        },
    }
}


class TestHardwareInfoInit:
    """Test HardwareInfo initialization."""

    def test_init_valid_data(self):
        """Test initialization with valid hardware data."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        assert hw_info.node == "test-vm-worker-1"
        assert hw_info.input_name == "test.json"
        assert isinstance(hw_info.data, dict)

    def test_init_missing_hardware_wrapper(self):
        """Test initialization fails without hardware wrapper."""
        invalid_data = {"data": {"id": "computer"}}
        with pytest.raises(ValueError) as exc_info:
            hw.HardwareInfo("test.json", invalid_data)
        assert "missing 'hardware' wrapper" in str(exc_info.value)

    def test_init_invalid_hardware_type(self):
        """Test initialization fails when hardware is not a dict."""
        invalid_data = {"hardware": "not a dict"}
        with pytest.raises(ValueError) as exc_info:
            hw.HardwareInfo("test.json", invalid_data)
        assert "missing 'hardware' wrapper" in str(exc_info.value)


class TestParseVendorString:
    """Test vendor string parsing."""

    def test_parse_vendor_with_id(self):
        """Test parsing vendor string with ID."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        vendor, vendor_id = hw_info._parse_vendor_string("Intel Corporation [8086]")
        assert vendor == "Intel Corporation"
        assert vendor_id == "8086"

    def test_parse_vendor_without_id(self):
        """Test parsing vendor string without ID."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        vendor, vendor_id = hw_info._parse_vendor_string("Red Hat")
        assert vendor == "Red Hat"
        assert vendor_id is None

    def test_parse_vendor_none(self):
        """Test parsing None vendor string."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        vendor, vendor_id = hw_info._parse_vendor_string(None)
        assert vendor is None
        assert vendor_id is None


class TestParseProductString:
    """Test product string parsing."""

    def test_parse_product_with_ids(self):
        """Test parsing product string with vendor:device IDs."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        model, vendor_id, device_id = hw_info._parse_product_string(
            "NetXtreme BCM5720 [14E4:165F]"
        )
        assert model == "NetXtreme BCM5720"
        assert vendor_id == "14E4"
        assert device_id == "165F"

    def test_parse_product_without_ids(self):
        """Test parsing product string without IDs."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        model, vendor_id, device_id = hw_info._parse_product_string("QEMU HARDDISK")
        assert model == "QEMU HARDDISK"
        assert vendor_id is None
        assert device_id is None

    def test_parse_product_none(self):
        """Test parsing None product string."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        model, vendor_id, device_id = hw_info._parse_product_string(None)
        assert model is None
        assert vendor_id is None
        assert device_id is None


class TestParseSystemModel:
    """Test system model string parsing."""

    def test_parse_dell_model(self):
        """Test parsing Dell model string with SKU."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        model, sku = hw_info._parse_system_model(
            "PowerEdge R750 (SKU=090E;ModelName=PowerEdge R750)"
        )
        assert model == "PowerEdge R750"
        assert sku == "090E"

    def test_parse_hpe_model(self):
        """Test parsing HPE model string with part number."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        model, sku = hw_info._parse_system_model("ProLiant DL110 Gen11 (P54277-B21)")
        assert model == "ProLiant DL110 Gen11"
        assert sku == "P54277-B21"

    def test_parse_kvm_model(self):
        """Test parsing KVM model string."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        model, sku = hw_info._parse_system_model("KVM (8.6.0)")
        assert model == "KVM"
        assert sku == "8.6.0"

    def test_parse_model_without_parenthesis(self):
        """Test parsing model string without parenthetical data."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        model, sku = hw_info._parse_system_model("Simple Model")
        assert model == "Simple Model"
        assert sku is None

    def test_parse_dell_not_provided_sku(self):
        """Test parsing Dell model with NotProvided SKU."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        model, sku = hw_info._parse_system_model(
            "PowerEdge R750 (SKU=NotProvided;ModelName=PowerEdge R750)"
        )
        assert model == "PowerEdge R750"
        assert sku is None


class TestParseFirmwareString:
    """Test firmware string parsing."""

    def test_parse_broadcom_firmware(self):
        """Test parsing Broadcom firmware string."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info._parse_firmware_string(
            "FFV21.80.8 bc 5720-v1.39", "Broadcom Inc."
        )
        assert result["primary"] == "FFV21.80.8"
        assert result["bootcode"] == "5720-v1.39"

    def test_parse_intel_firmware(self):
        """Test parsing Intel firmware string."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info._parse_firmware_string(
            "4.20 0x8001778b 22.0.9", "Intel Corporation"
        )
        assert result["primary"] == "4.20"
        assert result["nvm"] == "22.0.9"

    def test_parse_mellanox_firmware(self):
        """Test parsing Mellanox firmware string."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info._parse_firmware_string(
            "14.32.2004 (DEL0000000015)", "Mellanox Technologies"
        )
        assert result["primary"] == "14.32.2004"
        assert result["psid"] == "DEL0000000015"

    def test_parse_firmware_none(self):
        """Test parsing None firmware string."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info._parse_firmware_string(None, "Intel")
        assert result["primary"] is None


class TestIsVirtualFunction:
    """Test SR-IOV Virtual Function detection."""

    def test_physical_function(self):
        """Test PF detection (device=00)."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        assert hw_info._is_virtual_function("pci@0000:9d:00.0") is False
        assert hw_info._is_virtual_function("pci@0000:9d:00.3") is False

    def test_virtual_function(self):
        """Test VF detection (device!=00)."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        assert hw_info._is_virtual_function("pci@0000:9d:01.0") is True
        assert hw_info._is_virtual_function("pci@0000:9d:02.5") is True

    def test_non_pci_device(self):
        """Test non-PCI device returns False."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        assert hw_info._is_virtual_function("cpu@0") is False
        assert hw_info._is_virtual_function("scsi@0:0.0.0") is False
        assert hw_info._is_virtual_function(None) is False
        assert hw_info._is_virtual_function("") is False


class TestParseVM:
    """Test parsing VM hardware data."""

    def test_parse_vm_basic(self):
        """Test parsing VM returns all expected keys."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert result["node"] == "test-vm-worker-1"
        assert result["system_vendor"] == "Red Hat"
        assert result["system_model"] == "KVM"
        assert result["system_sku"] == "8.6.0"
        assert result["system_family"] == "Red Hat Enterprise Linux"

    def test_parse_vm_cpu(self):
        """Test parsing VM CPU information."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert result["cpu_vendor"] == "Intel Corp."
        assert "Xeon" in result["cpu_model"]
        assert result["cpu_sockets"] == 2
        assert result["cpu_total_cores"] == 8
        assert result["cpu_total_threads"] == 16
        assert result["cpu_frequency_mhz"] == 2200

    def test_parse_vm_memory(self):
        """Test parsing VM memory information."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert result["memory_total_gb"] == 64.0
        assert result["memory_dimm_count"] == 4

    def test_parse_vm_bios(self):
        """Test parsing VM BIOS information."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert result["bios_vendor"] == "EFI Development Kit II / OVMF"
        assert result["bios_version"] == "0.0.0"
        assert result["bios_type"] == "UEFI"

    def test_parse_vm_network(self):
        """Test parsing VM network interfaces."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert len(result["network_interfaces"]) >= 1
        nic = result["network_interfaces"][0]
        assert nic["description"] == "Ethernet controller"
        assert "Red Hat" in nic["vendor"]

    def test_parse_vm_storage(self):
        """Test parsing VM storage devices."""
        hw_info = hw.HardwareInfo("test.json", VM_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert len(result["storage_devices"]) >= 1
        disk = result["storage_devices"][0]
        assert disk["vendor"] == "QEMU"
        assert disk["size_gb"] == 100.0


class TestParseBareMetal:
    """Test parsing bare metal hardware data."""

    def test_parse_bare_metal_basic(self):
        """Test parsing bare metal returns all expected keys."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert result["node"] == "server-sno-01"
        assert result["system_vendor"] == "Dell Inc."
        assert result["system_model"] == "PowerEdge R750"
        assert result["system_sku"] == "090E"
        assert result["system_family"] == "PowerEdge"

    def test_parse_bare_metal_cpu(self):
        """Test parsing bare metal CPU information."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert "Intel" in result["cpu_vendor"]
        assert "6338N" in result["cpu_model"]
        assert result["cpu_sockets"] == 2
        assert result["cpu_total_cores"] == 64
        assert result["cpu_total_threads"] == 128

    def test_parse_bare_metal_memory(self):
        """Test parsing bare metal memory information."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        # Two memory nodes each with 256 GB
        assert result["memory_total_gb"] == 512.0
        assert result["memory_dimm_count"] == 4

    def test_parse_bare_metal_network_broadcom(self):
        """Test parsing Broadcom NIC with firmware parsing."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        broadcom_nics = [
            n for n in result["network_interfaces"] if "BCM5720" in (n["model"] or "")
        ]
        assert len(broadcom_nics) >= 1
        nic = broadcom_nics[0]
        assert nic["driver"] == "tg3"
        assert nic["firmware_version"] == "FFV21.80.8"
        assert nic["firmware_bootcode"] == "5720-v1.39"

    def test_parse_bare_metal_network_intel(self):
        """Test parsing Intel NIC with firmware parsing."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        intel_nics = [
            n for n in result["network_interfaces"] if "E810" in (n["model"] or "")
        ]
        assert len(intel_nics) >= 1
        nic = intel_nics[0]
        assert nic["driver"] == "ice"
        assert nic["firmware_version"] == "4.20"
        assert nic["firmware_nvm"] == "22.0.9"

    def test_parse_bare_metal_network_mellanox(self):
        """Test parsing Mellanox NIC with firmware parsing."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        mlx_nics = [
            n
            for n in result["network_interfaces"]
            if "ConnectX-4" in (n["model"] or "")
            and "Virtual" not in (n["model"] or "")
        ]
        assert len(mlx_nics) >= 1
        nic = mlx_nics[0]
        assert nic["driver"] == "mlx5_core"
        assert nic["firmware_version"] == "14.32.2004"
        assert nic["firmware_psid"] == "DEL0000000015"

    def test_parse_bare_metal_virtual_function(self):
        """Test SR-IOV VF detection in bare metal."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        vf_nics = [n for n in result["network_interfaces"] if n["is_virtual_function"]]
        pf_nics = [
            n for n in result["network_interfaces"] if not n["is_virtual_function"]
        ]

        assert len(vf_nics) >= 1
        assert len(pf_nics) >= 3

    def test_parse_bare_metal_storage(self):
        """Test parsing bare metal storage (NVMe)."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        nvme_disks = [d for d in result["storage_devices"] if d["type"] == "nvme"]
        assert len(nvme_disks) >= 1
        nvme = nvme_disks[0]
        assert nvme["size_gb"] == pytest.approx(1788.5, rel=0.1)

    def test_parse_bare_metal_pci_accelerators(self):
        """Test parsing PCI accelerators."""
        hw_info = hw.HardwareInfo("test.json", BARE_METAL_HARDWARE_SAMPLE)
        result = hw_info.parse()

        assert len(result["pci_accelerators"]) >= 1
        acc = result["pci_accelerators"][0]
        assert "ACC100" in acc["model"]


class TestNormalizeFunction:
    """Test the normalize() function."""

    def test_normalize_success(self):
        """Test normalize returns parsed data."""
        result = hw.normalize("test.json", VM_HARDWARE_SAMPLE)
        assert result is not None
        assert result["node"] == "test-vm-worker-1"

    def test_normalize_invalid_data(self):
        """Test normalize returns None for invalid data."""
        result = hw.normalize("test.json", {"invalid": "data"})
        assert result is None

    def test_normalize_bare_metal(self):
        """Test normalize with bare metal data."""
        result = hw.normalize("baremetal.json", BARE_METAL_HARDWARE_SAMPLE)
        assert result is not None
        assert result["system_vendor"] == "Dell Inc."


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_children(self):
        """Test parsing with empty children arrays."""
        data = {
            "hardware": {
                "node": "test",
                "data": {
                    "id": "computer",
                    "class": "system",
                    "children": [],
                },
            }
        }
        hw_info = hw.HardwareInfo("test.json", data)
        result = hw_info.parse()
        assert result["node"] == "test"
        assert result["cpu_sockets"] == 0
        assert result["memory_total_gb"] == 0.0

    def test_missing_configuration(self):
        """Test parsing when configuration is missing."""
        data = {
            "hardware": {
                "node": "test",
                "data": {
                    "id": "computer",
                    "class": "system",
                    "product": "Test Product",
                    # No configuration key
                },
            }
        }
        hw_info = hw.HardwareInfo("test.json", data)
        result = hw_info.parse()
        assert result["system_family"] is None

    def test_cores_threads_as_strings(self):
        """Test parsing cores/threads when they are strings."""
        data = {
            "hardware": {
                "node": "test",
                "data": {
                    "id": "computer",
                    "class": "system",
                    "children": [
                        {
                            "id": "core",
                            "class": "bus",
                            "children": [
                                {
                                    "id": "cpu:0",
                                    "class": "processor",
                                    "configuration": {
                                        "cores": "8",  # String
                                        "threads": "16",  # String
                                    },
                                }
                            ],
                        }
                    ],
                },
            }
        }
        hw_info = hw.HardwareInfo("test.json", data)
        result = hw_info.parse()
        assert result["cpu_total_cores"] == 8
        assert result["cpu_total_threads"] == 16
