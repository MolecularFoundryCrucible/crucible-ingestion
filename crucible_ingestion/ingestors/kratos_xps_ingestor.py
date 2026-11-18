from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
from PIL import Image

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

B30_XPS_INSTRUMENT_MFID = '0tqx1eq8b5t8h000y3zm23mwmw'
B30_XPS_INSTRUMENT_ID = 'b30_xps'
B30_XPS_INSTRUMENT_NAME = 'B30 XPS'
KAL_DATE_FORMATS = (
    '%y/%m/%d %H:%M:%S',
    '%Y-%m-%d %H:%M:%S',
    '%d/%m/%y %H:%M:%S',
    '%m/%d/%y %H:%M:%S',
)
MAX_THUMBNAILS = 25
XRAY_PHOTON_ENERGY_EV = {
    'F_REFER_TO_XRAY_MONO_AL': 1486.69,
    'F_NICPU_XRAY_ANODE_STD_MONO': 1486.69,
    'F_REFER_TO_XRAY_AL': 1486.6,
    'F_REFER_TO_XRAY_MG': 1253.6,
    'F_REFER_TO_XRAY_MONO_AG': 2984.3,
    'F_NICPU_XRAY_ANODE_AG_MONO': 2984.3,
    'F_REFER_TO_XRAY_MONO_CR': 5414.8,
    'F_NICPU_XRAY_ANODE_CR_MONO': 5414.8,
}

_INLINE_ARRAY_RE = re.compile(r"^\{(.*)\}$")


class KalFormatError(ValueError):
    pass


def _try_number(text: str) -> Any:
    text = text.strip()
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def _parse_value(text: str) -> Any:
    """Split 'number unit' into (number, unit); leave plain strings alone."""
    text = text.strip()
    if not text:
        return None
    match = re.match(r"^(-?\d+\.?\d*(?:[eE][-+]?\d+)?)\s+([A-Za-z/%_.]+)$", text)
    if match:
        return {"value": _try_number(match.group(1)), "unit": match.group(2)}
    return _try_number(text)


def _parse_inline_array(inner: str) -> list[Any]:
    if not inner.strip():
        return []
    return [_try_number(item) for item in inner.split(",")]


def _unwrap(value: Any) -> Any:
    """Value or {'value': .., 'unit': ..} -> bare numeric/string value."""
    if isinstance(value, dict) and set(value.keys()) == {"value", "unit"}:
        return value["value"]
    return value


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict) and set(value.keys()) == {"value", "unit"}:
        return f"{value['value']} {value['unit']}".strip() or None
    if isinstance(value, (dict, list)):
        return None
    s = str(value).strip()
    return s or None


def _as_float(value: Any) -> float | None:
    raw = _unwrap(value)
    if raw is None or isinstance(raw, (bool, dict, list)):
        return None
    try:
        num = float(raw)
        return num if math.isfinite(num) else None
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    raw = _unwrap(value)
    if raw is None or isinstance(raw, (bool, dict, list)):
        return None
    if isinstance(raw, int):
        return raw
    try:
        num = float(raw)
        if math.isfinite(num) and num.is_integer():
            return int(num)
        return None
    except (TypeError, ValueError):
        return None


def _format_anode_material(raw: Any) -> str | None:
    if not raw:
        return None
    raw_str = str(raw).strip()
    mapping = {
        "F_NICPU_XRAY_ANODE_STD_MONO": "Al Mono",
        "F_NICPU_XRAY_ANODE_STD_DUAL": "Al/Mg Dual",
        "F_NICPU_XRAY_ANODE_AG_MONO": "Ag Mono",
        "F_NICPU_XRAY_ANODE_CR_MONO": "Cr Mono",
    }
    return mapping.get(raw_str, raw_str)


def unique_name(name: str, taken: set[str] | list[str] | dict[str, Any]) -> str:
    candidate, n = name, 2
    while candidate in taken:
        candidate, n = f"{name} ({n})", n + 1
    return candidate


def parse_kal(text: str) -> list[dict[str, Any]]:
    """Parse .kal text into a list of raw objects."""
    if "\x00" in text:
        raise KalFormatError("binary content in .kal text")

    objects: list[dict[str, Any]] = []
    dataset_filename: str | None = None
    dataset_comment: str | None = None
    current_fields: dict[str, Any] | None = None
    stack: list[dict[str, Any] | list[Any]] = []

    lines = text.splitlines()
    n = len(lines)
    i = 0
    while i < n:
        line = lines[i]
        stripped = line.strip()
        i += 1
        if not stripped:
            continue

        if stripped == "{":
            if not stack:
                raise KalFormatError("unexpected opening brace before object")
            if not isinstance(stack[-1], list):
                raise KalFormatError("unexpected standalone opening brace outside list")
            new_obj: dict[str, Any] = {}
            stack[-1].append(new_obj)
            stack.append(new_obj)
            continue

        if stripped == "}":
            if len(stack) <= 1:
                raise KalFormatError("unmatched closing brace")
            stack.pop()
            continue

        if "=" not in line:
            continue

        left, _, right = line.partition("=")
        left = left.strip()
        value_text = right.strip()

        id_match = re.match(r"^(\d+)\s+(.*)$", left)
        key = id_match.group(2).strip() if id_match else left

        if key == "Dataset filename":
            dataset_filename = value_text
            continue
        if key == "Dataset Comment":
            dataset_comment = value_text
            continue
        if key == "Object name":
            if objects and len(stack) != 1:
                raise KalFormatError("unclosed brace block")
            head, sep, tail = value_text.rpartition("/")
            if sep and tail.isdigit():
                name, obj_index = head, int(tail)
            else:
                name, obj_index = value_text, None
            current_fields = {}
            objects.append(
                {
                    "name": name,
                    "index": obj_index,
                    "dataset_filename": dataset_filename,
                    "dataset_comment": dataset_comment,
                    "fields": current_fields,
                }
            )
            stack = [current_fields]
            continue

        if not stack:
            raise KalFormatError(f"field {key!r} outside object")

        target = stack[-1]
        array_match = _INLINE_ARRAY_RE.match(value_text)
        if array_match:
            parsed: Any = _parse_inline_array(array_match.group(1))
        elif value_text == "{":
            j = i
            while j < n and not lines[j].strip():
                j += 1
            is_list = j < n and lines[j].strip() == "{"
            parsed = [] if is_list else {}
            stack.append(parsed)
        else:
            parsed = _parse_value(value_text)

        if isinstance(target, dict):
            target[key] = parsed
        else:
            target.append(parsed)

    if objects and len(stack) != 1:
        raise KalFormatError("unclosed brace block")

    if not objects:
        raise KalFormatError("no Kratos objects found")

    return objects


def parse_kal_date(raw: Any) -> datetime | None:
    if not raw:
        return None
    raw_str = str(raw).strip()
    if not raw_str:
        return None
    for fmt in KAL_DATE_FORMATS:
        try:
            return datetime.strptime(raw_str, fmt)
        except ValueError:
            pass
    return None


def format_kal_date(raw: Any) -> str | None:
    if not raw:
        return None
    dt = parse_kal_date(raw)
    if dt is not None:
        return dt.isoformat()
    s = str(raw).strip()
    return s or None


def resolve_photon_energy_ev(*candidates: Any) -> float | None:
    for cand in candidates:
        if cand is None:
            continue
        cand_str = str(cand).strip()
        if cand_str in XRAY_PHOTON_ENERGY_EV:
            return XRAY_PHOTON_ENERGY_EV[cand_str]
    return None


@dataclass
class XPSSpectrum:
    region: str
    index: int | None
    x: np.ndarray
    y: np.ndarray
    start_energy: float | None = None
    step_size: float | None = None
    abscissa_label: str | None = None
    abscissa_unit: str | None = None
    ordinate_label: str | None = None
    ordinate_unit: str | None = None
    technique: str | None = None
    scan_type: str | None = None
    dwell_time_s: float | None = None
    pass_energy_eV: float | None = None
    sweeps_completed: int | None = None
    date_acquired: str | None = None
    chemical_symbol: str | None = None
    transition: str | None = None
    work_function: float | None = None
    sample_bias_eV: float | None = None
    stage_position_m: dict[str, float] = field(default_factory=dict)
    analyser_pass_energy: str | None = None
    xray_reference_energy: str | None = None
    aperture_size: str | None = None
    iris_position: str | None = None
    type_of_analyser: str | None = None
    photon_energy_eV: float | None = None
    fields: dict[str, Any] = field(default_factory=dict, repr=False)

    def binding_energy(self) -> np.ndarray | None:
        label = (str(self.abscissa_label) if self.abscissa_label is not None else "").strip().lower()
        unit = (str(self.abscissa_unit) if self.abscissa_unit is not None else "ev").strip().lower()
        if label == "binding energy" and unit == "ev":
            return self.x
        if label == "kinetic energy" and unit == "ev" and self.photon_energy_eV is not None:
            return self.photon_energy_eV - self.x
        return None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "region": self.region,
            "index": self.index,
            "technique": self.technique,
            "scan_type": self.scan_type,
        }
        if self.pass_energy_eV is not None:
            data["pass_energy_eV"] = self.pass_energy_eV
        if self.analyser_pass_energy is not None:
            data["analyser_pass_energy"] = self.analyser_pass_energy
        if self.xray_reference_energy is not None:
            data["xray_reference_energy"] = self.xray_reference_energy
        if self.aperture_size is not None:
            data["aperture_size"] = self.aperture_size
        if self.iris_position is not None:
            data["iris_position"] = self.iris_position
        if self.type_of_analyser is not None:
            data["type_of_analyser"] = self.type_of_analyser

        data["dwell_time_s"] = self.dwell_time_s
        data["sweeps_completed"] = self.sweeps_completed
        data["date_acquired"] = format_kal_date(self.date_acquired)
        data["abscissa_label"] = self.abscissa_label
        data["abscissa_unit"] = self.abscissa_unit
        data["ordinate_label"] = self.ordinate_label
        data["ordinate_unit"] = self.ordinate_unit
        data["start_energy"] = self.start_energy
        data["step_size"] = self.step_size
        data["num_points"] = int(self.y.size)

        be = self.binding_energy()
        if be is not None:
            data["binding_energy_range_eV"] = [round(float(be[0]), 6), round(float(be[-1]), 6)]

        if self.chemical_symbol is not None:
            data["chemical_symbol"] = self.chemical_symbol
        if self.transition is not None:
            data["transition"] = self.transition
        if self.work_function is not None:
            data["work_function"] = self.work_function
        if self.sample_bias_eV is not None:
            data["sample_bias_eV"] = self.sample_bias_eV
        if self.stage_position_m:
            data["stage_position_m"] = self.stage_position_m

        return data


def _build_spectrum(obj: dict[str, Any], fallback_photon_energy_ev: float | None = None) -> XPSSpectrum:
    fields = obj.get("fields", {})
    if "Ordinate values" not in fields:
        raise KalFormatError(f"spectrum object {obj.get('name')!r} missing 'Ordinate values'")

    raw_y = fields["Ordinate values"]
    if not isinstance(raw_y, (list, tuple)) or len(raw_y) < 2:
        raise KalFormatError(
            f"spectrum {obj.get('name')!r} ordinate values must have length >= 2"
        )

    try:
        y = np.asarray(raw_y, dtype=float)
    except (TypeError, ValueError) as err:
        raise KalFormatError(f"invalid ordinate values in {obj.get('name')!r}: {err}") from err

    if not np.all(np.isfinite(y)):
        raise KalFormatError(f"non-finite ordinate values in {obj.get('name')!r}")

    start_raw = _unwrap(fields.get("Spectrum scan start", 0.0))
    step_raw = _unwrap(fields.get("Spectrum scan step size", 1.0))
    try:
        start = float(start_raw) if start_raw is not None else 0.0
        step = float(step_raw) if step_raw is not None else 1.0
    except (TypeError, ValueError) as err:
        raise KalFormatError(f"invalid scan start or step size in {obj.get('name')!r}: {err}") from err

    if not (np.isfinite(start) and np.isfinite(step) and step != 0):
        raise KalFormatError(
            f"start ({start}) and step ({step}) must be finite with step != 0 in {obj.get('name')!r}"
        )

    x = start + step * np.arange(y.size)

    photon_energy = resolve_photon_energy_ev(
        fields.get("Xray Reference Energy"),
        fields.get("NICPU X.Gun Energy Reference"),
        fields.get("NICPU X-ray Gun Active Anode Material"),
        fallback_photon_energy_ev,
    )

    stage_position = {}
    for axis, key in (
        ("x", "Stage X Position"),
        ("y", "Stage Y Position"),
        ("z", "Stage Z Position"),
    ):
        val = _as_float(fields.get(key))
        if val is not None:
            stage_position[axis] = val

    pass_energy = _as_float(fields.get("Pass energy"))
    dwell_time = _as_float(fields.get("Dwell time"))
    sweeps = _as_int(fields.get("# Sweeps completed"))
    work_func = _as_float(fields.get("Work function"))
    sample_bias = _as_float(fields.get("Sample Bias"))

    technique = _as_str(fields.get("Technique"))
    if technique and technique.startswith("F_"):
        technique = technique[2:]

    scan_type = _as_str(fields.get("Scan type"))
    if scan_type and scan_type.startswith("F_"):
        scan_type = scan_type[2:]

    return XPSSpectrum(
        region=str(obj.get("name", "")),
        index=_as_int(obj.get("index")),
        x=x,
        y=y,
        start_energy=float(start),
        step_size=float(step),
        abscissa_label=_as_str(fields.get("Abscissa label")),
        abscissa_unit=_as_str(fields.get("Abscissa units")),
        ordinate_label=_as_str(fields.get("Ordinate label")),
        ordinate_unit=_as_str(fields.get("Ordinate units")),
        technique=technique,
        scan_type=scan_type,
        dwell_time_s=dwell_time,
        pass_energy_eV=pass_energy,
        sweeps_completed=sweeps,
        date_acquired=_as_str(fields.get("Date Acquired")),
        chemical_symbol=_as_str(fields.get("Chemical symbol or formula")),
        transition=_as_str(fields.get("Transition or charge state")),
        work_function=work_func,
        sample_bias_eV=sample_bias,
        stage_position_m=stage_position,
        analyser_pass_energy=_as_str(fields.get("Analyser Pass Energy")),
        xray_reference_energy=_as_str(fields.get("Xray Reference Energy")),
        aperture_size=_as_str(fields.get("Descriptor for aperture size used in acquisition")),
        iris_position=_as_str(fields.get("Descriptor for iris position used in acquisition")),
        type_of_analyser=_as_str(fields.get("Type Of Analyser")),
        photon_energy_eV=photon_energy,
        fields=fields,
    )


@dataclass
class XPSFile:
    path: Path | None
    dataset_filename: str | None
    dataset_comment: str | None
    spectra: list[XPSSpectrum]
    events: list[dict[str, Any]]

    def get_spectra_summary(self) -> list[str]:
        groups: dict[tuple[str, Any, Any, Any], int] = {}
        for s in self.spectra:
            key = (s.region, s.pass_energy_eV, s.sweeps_completed, s.aperture_size)
            groups[key] = groups.get(key, 0) + 1

        summary_strings: list[str] = []
        for (region, pe, sweeps, aperture), count in groups.items():
            parts = [f"{count}x"]
            if pe is not None:
                pe_str = f"{pe:g}" if isinstance(pe, (int, float)) else str(pe)
                parts.append(f"pass energy {pe_str} eV")
            if sweeps is not None:
                parts.append(f"sweeps {sweeps}")
            if aperture is not None:
                parts.append(f"Aperture {aperture}")
            summary_strings.append(f"{region} ({', '.join(parts)})")

        return summary_strings

    def extract_header(self, source_file: str) -> dict[str, Any]:
        anode_raw = None
        voltage = None
        current = None
        photon_energy = None
        techniques_set: set[str] = set()

        for s in self.spectra:
            if s.technique:
                clean_tech = s.technique[2:] if s.technique.startswith("F_") else s.technique
                techniques_set.add(clean_tech)
            if anode_raw is None:
                cand_anode = _as_str(s.fields.get("NICPU X-ray Gun Active Anode Material"))
                if cand_anode is not None:
                    anode_raw = cand_anode
            if voltage is None:
                cand_v = _as_float(s.fields.get("NICPU X-ray Gun Anode HT Voltage"))
                if cand_v is not None:
                    voltage = cand_v
            if current is None:
                cand_c = _as_float(s.fields.get("NICPU X-ray Gun Emission Current"))
                if cand_c is not None:
                    current = cand_c
            if photon_energy is None:
                cand = s.photon_energy_eV or resolve_photon_energy_ev(
                    s.fields.get("Xray Reference Energy"),
                    s.fields.get("NICPU X.Gun Energy Reference"),
                    s.fields.get("NICPU X-ray Gun Active Anode Material"),
                )
                if cand is not None:
                    photon_energy = cand

        if anode_raw is None or voltage is None or current is None or photon_energy is None:
            for ev in self.events:
                f = ev.get("fields", {}) if isinstance(ev.get("fields"), dict) else {}
                scan_obj = f.get("Scan Sweep OBJECT", {}) if isinstance(f.get("Scan Sweep OBJECT"), dict) else {}
                if anode_raw is None:
                    cand_anode = _as_str(f.get("NICPU X-ray Gun Active Anode Material"))
                    if cand_anode is None:
                        cand_anode = _as_str(scan_obj.get("NICPU X-ray Gun Active Anode Material"))
                    if cand_anode is not None:
                        anode_raw = cand_anode
                if voltage is None:
                    cand_v = _as_float(f.get("NICPU X-ray Gun Anode HT Voltage"))
                    if cand_v is None:
                        cand_v = _as_float(scan_obj.get("NICPU X-ray Gun Anode HT Voltage"))
                    if cand_v is not None:
                        voltage = cand_v
                if current is None:
                    cand_c = _as_float(f.get("NICPU X-ray Gun Emission Current"))
                    if cand_c is None:
                        cand_c = _as_float(scan_obj.get("NICPU X-ray Gun Emission Current"))
                    if cand_c is not None:
                        current = cand_c
                if photon_energy is None:
                    cand_pe = resolve_photon_energy_ev(
                        f.get("Xray Reference Energy"),
                        f.get("NICPU X.Gun Energy Reference"),
                        f.get("NICPU X-ray Gun Active Anode Material"),
                        scan_obj.get("Xray Reference Energy"),
                        scan_obj.get("NICPU X.Gun Energy Reference"),
                        scan_obj.get("NICPU X-ray Gun Active Anode Material"),
                    )
                    if cand_pe is not None:
                        photon_energy = cand_pe

        stage_position_name = None
        for ev in self.events:
            f = ev.get("fields", {}) if isinstance(ev.get("fields"), dict) else {}
            name = _as_str(f.get("Stage Position Name"))
            if name is not None:
                stage_position_name = name
                break

        dataset_comment = str(self.dataset_comment).strip() if self.dataset_comment else None

        times = [t for t in (parse_kal_date(s.date_acquired) for s in self.spectra) if t is not None]
        if times:
            date_acquired = min(times).isoformat()
        else:
            date_acquired = next(
                (fmt for s in self.spectra if (fmt := format_kal_date(s.date_acquired))),
                None,
            )

        raw_header = {
            "source_file": source_file,
            "dataset_filename": self.dataset_filename,
            "dataset_comment": dataset_comment,
            "stage_position_name": stage_position_name,
            "date_acquired": date_acquired,
            "technique": ", ".join(sorted(techniques_set)) if techniques_set else None,
            "anode_material": _format_anode_material(anode_raw),
            "anode_voltage_V": voltage,
            "emission_current_A": current,
            "photon_energy_eV": photon_energy,
            "total_spectra": len(self.spectra),
            "spectra_summary": self.get_spectra_summary(),
        }
        return {k: v for k, v in raw_header.items() if v is not None and v != ""}

    def extract_events(self) -> list[dict[str, Any]]:
        etch_events: list[dict[str, Any]] = []
        cycle = 1
        for ev in self.events:
            if ev.get("name") == "Ion Gun Etch":
                f = ev.get("fields", {}) if isinstance(ev.get("fields"), dict) else {}
                etch_events.append({
                    "event_type": "Ion Gun Etch",
                    "cycle": cycle,
                    "index": ev.get("index"),
                    "etch_time_s": _as_float(f.get("Etch time")),
                    "pre_etch_delay_s": _as_float(f.get("Pre-etch delay")),
                    "post_etch_delay_s": _as_float(f.get("Post etch delay")),
                    "beam_voltage_V": _as_float(f.get("NICPU Ion Gun PSU beam_ht")),
                    "emission_current_A": _as_float(f.get("NICPU Ion Gun PSU Emission Current")),
                    "extractor_voltage_V": _as_float(f.get("NICPU Ion Gun PSU Extractor Voltage")),
                    "raster_size_mm": _as_float(f.get("NICPU Ion Gun PSU Raster Size")),
                    "standby_delay_s": _as_float(f.get("NICPU Ion Gun Standby Delay Time")),
                })
                cycle += 1
        return etch_events


def load_kal_file(path: str | Path) -> XPSFile:
    file_path = Path(path)
    data = file_path.read_bytes()
    if b"\x00" in data:
        raise KalFormatError("binary content in .kal file")
    text = data.decode("utf-8", errors="replace")
    objects = parse_kal(text)

    spectra_objs: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for obj in objects:
        fields = obj.get("fields")
        if isinstance(fields, dict) and "Ordinate values" in fields:
            spectra_objs.append(obj)
        else:
            events.append(obj)

    fallback_photon_energy_ev = None
    for ev in events:
        f = ev.get("fields", {}) if isinstance(ev.get("fields"), dict) else {}
        scan_obj = f.get("Scan Sweep OBJECT", {}) if isinstance(f.get("Scan Sweep OBJECT"), dict) else {}
        for cand in (
            f.get("Xray Reference Energy"),
            f.get("NICPU X.Gun Energy Reference"),
            f.get("NICPU X-ray Gun Active Anode Material"),
            scan_obj.get("Xray Reference Energy"),
            scan_obj.get("NICPU X.Gun Energy Reference"),
            scan_obj.get("NICPU X-ray Gun Active Anode Material"),
        ):
            energy = resolve_photon_energy_ev(cand)
            if energy is not None:
                fallback_photon_energy_ev = energy
                break
        if fallback_photon_energy_ev is not None:
            break

    spectra = [_build_spectrum(obj, fallback_photon_energy_ev=fallback_photon_energy_ev) for obj in spectra_objs]
    if not spectra:
        raise KalFormatError("no XPS spectra found")

    dataset_filename = objects[0].get("dataset_filename") if objects else None
    dataset_comment = objects[0].get("dataset_comment") if objects else None

    return XPSFile(
        path=file_path,
        dataset_filename=dataset_filename,
        dataset_comment=dataset_comment,
        spectra=spectra,
        events=events,
    )


class KratosXpsIngestor(CrucibleDatasetIngestor):

    def _ensure_file_loaded(self) -> XPSFile:
        xps_file = getattr(self, "xps_file", None)
        if xps_file is None:
            xps_file = load_kal_file(self.file_to_upload)
            self.xps_file = xps_file
        return xps_file

    def is_file_supported(self) -> bool:
        if not self.match_name.lower().endswith(".kal"):
            return False
        try:
            self.xps_file = load_kal_file(self.file_to_upload)
        except (KalFormatError, ValueError, OSError) as err:
            logger.info(f"{self.match_name} is not a valid Kratos .kal file: {err}")
            return False
        return True

    def parse_instrument(self) -> None:
        if not (self.instrument_mfid or self.instrument_id or self.instrument_name):
            self.instrument_name = B30_XPS_INSTRUMENT_NAME
            self.instrument_id = B30_XPS_INSTRUMENT_ID
            self.instrument_mfid = B30_XPS_INSTRUMENT_MFID
        super().parse_instrument()

    def parse_measurement(self) -> None:
        self.measurement = "XPS"

    def parse_data_type(self) -> None:
        self.data_type = "XPS Spectrum"

    def parse_file_timestamp(self) -> None:
        self._ensure_file_loaded()
        times = [t for t in (parse_kal_date(s.date_acquired) for s in self.xps_file.spectra) if t is not None]
        if times:
            self.timestamp = min(times).isoformat()
        else:
            CrucibleDatasetIngestor.parse_file_timestamp(self)

    def get_scientific_metadata(self) -> None:
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        self._ensure_file_loaded()
        self.scientific_metadata.update(self.xps_file.extract_header(Path(self.match_name).name))
        self.scientific_metadata["spectra"] = [s.to_dict() for s in self.xps_file.spectra]
        etch_events = self.xps_file.extract_events()
        if etch_events:
            self.scientific_metadata["events"] = etch_events

    def get_thumbnails(self) -> None:
        self.thumbnails = []
        self._ensure_file_loaded()
        seen: set[str] = set()
        for spec in self.xps_file.spectra[:MAX_THUMBNAILS]:
            caption = unique_name(spec.region, seen)
            seen.add(caption)
            fig, ax = plt.subplots()
            try:
                be = spec.binding_energy()
                if be is not None:
                    ax.plot(be, spec.y, color="tab:blue", linewidth=0.8)
                    ax.set_xlim(float(max(be[0], be[-1])), float(min(be[0], be[-1])))
                    ax.set_xlabel("Binding Energy (eV)")
                else:
                    ax.plot(spec.x, spec.y, color="tab:blue", linewidth=0.8)
                    label = spec.abscissa_label or "Energy"
                    unit = spec.abscissa_unit
                    ax.set_xlabel(f"{label} ({unit})" if unit else label)

                if spec.ordinate_label == "Intensity" and spec.ordinate_unit == "counts":
                    ylabel = "Intensity (counts)"
                elif spec.ordinate_label and spec.ordinate_unit:
                    ylabel = f"{spec.ordinate_label} ({spec.ordinate_unit})"
                elif spec.ordinate_label:
                    ylabel = spec.ordinate_label
                else:
                    ylabel = "Intensity"

                ax.set_ylabel(ylabel)
                ax.set_title(caption)

                buf = BytesIO()
                fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
                buf.seek(0)
                self.add_thumbnail(Image.open(buf), caption, size=(500, 500))
            except Exception as err:
                logger.error(f"Failed to generate XPS thumbnail for {caption!r}: {err}")
            finally:
                plt.close(fig)
