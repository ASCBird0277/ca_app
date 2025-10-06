from __future__ import annotations

import hashlib
import html
import logging
import re
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
import yaml
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from rapidfuzz import fuzz, process

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("refab_map_app")


class DataStore:
    """In-memory data facade for properties, positions, and employees."""

    SCORE_CUTOFF = 60
    MAX_SEARCH_RESULTS = 120
    MAX_EMPLOYEE_MATCHES = 40
    CARD_EST_HEIGHT = 144
    STOPWORDS = {"a", "an", "and", "the", "of", "on", "at", "for", "to", "in", "by", "with"}

    def __init__(self, data_dir: Path, config_path: Path) -> None:
        self.data_dir = data_dir
        self.config_path = config_path
        self.lock = threading.Lock()
        self.config: Dict[str, Any] = {}
        self.flags: Dict[str, Any] = {}
        self.employees_lookup: Dict[str, Dict[str, Any]] = {}
        self.properties_payload: Dict[str, Dict[str, Any]] = {}
        self.positions_by_property: Dict[str, List[Dict[str, Any]]] = {}
        self.property_order: List[str] = []
        self.name_to_property_id: Dict[str, str] = {}
        self.search_corpus: Dict[str, str] = {}
        self.region_set: set[str] = set()
        self.last_stats: Dict[str, Any] = {}
        self.reload()

    def reload(self) -> Dict[str, Any]:
        """Reload Excel data into memory, returning summary stats."""
        with self.lock:
            logger.info("Reloading Excel data from %s", self.data_dir)
            self.config = self._load_config()
            mappings = self.config.get("mappings", {})
            if not mappings:
                raise ValueError("config.yaml is missing a 'mappings' section")

            employees_raw = self._read_excel("Employee.xlsx")
            properties_raw = self._read_excel("Properties_geocoded.xlsx")
            positions_raw = self._read_excel("Positions.xlsx")

            employees_norm = self._normalize_dataframe(
                employees_raw, mappings.get("employees", {})
            )
            properties_norm = self._normalize_dataframe(
                properties_raw, mappings.get("properties", {})
            )
            positions_norm = self._normalize_dataframe(
                positions_raw, mappings.get("positions", {})
            )

            self.flags = self.config.get("flags", {})
            self.employees_lookup = self._prepare_employees(employees_norm)
            (
                properties_payload,
                property_order,
                name_to_id,
                region_set,
                property_stats,
            ) = self._prepare_properties(properties_norm)
            positions_by_property, position_stats = self._prepare_positions(
                positions_norm, properties_payload, name_to_id
            )
            self._finalize_properties(properties_payload, positions_by_property)

            self.properties_payload = properties_payload
            self.property_order = property_order
            self.positions_by_property = positions_by_property
            self.name_to_property_id = name_to_id
            self.region_set = region_set
            self.search_corpus = self._build_search_corpus(
                properties_payload, positions_by_property
            )
            self.last_stats = {
                "employees": len(self.employees_lookup),
                "properties": len(self.properties_payload),
                "positions": sum(len(v) for v in positions_by_property.values()),
                "skipped_properties": property_stats["skipped"],
                "skipped_positions": position_stats["skipped"],
            }
            logger.info(
                "Loaded %s properties, %s employees, %s positions",
                self.last_stats["properties"],
                self.last_stats["employees"],
                self.last_stats["positions"],
            )
            return dict(self.last_stats)

    def get_properties(self) -> List[Dict[str, Any]]:
        with self.lock:
            return [self._copy_property(pid) for pid in self.property_order]

    def get_regions(self) -> List[str]:
        with self.lock:
            return sorted(self.region_set)


    def search_properties(
        self, query: Optional[str], filters: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        with self.lock:
            trimmed = (query or "").strip()
            tokens_raw = re.split(r"\s+", trimmed.casefold())
            tokens = [token for token in tokens_raw if token and token not in self.STOPWORDS]
            query_fold = trimmed.casefold()
            employee_match_cache: Dict[str, List[Dict[str, Any]]] = {}
            employee_candidate_ids: set[str] = set()

    
            def _field_text(property_id: str) -> str:
                record = self.properties_payload.get(property_id, {})
                return " ".join(
                    filter(
                        None,
                        [
                            record.get("property"),
                            record.get("address"),
                            record.get("city"),
                            record.get("region"),
                        ],
                    )
                ).casefold()
    
            candidate_ids: List[str]
            if not trimmed:
                candidate_ids = list(self.property_order)
            else:
                extraction = process.extract(
                    trimmed,
                    self.search_corpus,
                    scorer=fuzz.WRatio,
                    score_cutoff=self.SCORE_CUTOFF,
                    limit=self.MAX_SEARCH_RESULTS,
                )
                candidate_ids = [match[2] for match in extraction]
                for pid in self.property_order:
                    matches = self._collect_employee_matches(pid, trimmed)
                    if matches:
                        employee_candidate_ids.add(pid)
                        employee_match_cache[pid] = matches
                if employee_candidate_ids:
                    seen_candidates = set(candidate_ids)
                    for pid in self.property_order:
                        if pid in employee_candidate_ids and pid not in seen_candidates:
                            candidate_ids.append(pid)
                            seen_candidates.add(pid)

    
                if not candidate_ids and query_fold:
                    candidate_ids = [
                        pid
                        for pid in self.property_order
                        if query_fold in self.search_corpus.get(pid, "").casefold()
                    ]
    
            if candidate_ids:
                field_cache = {pid: _field_text(pid) for pid in candidate_ids}
                corpus_cache = {pid: self.search_corpus.get(pid, '').casefold() for pid in candidate_ids}
                if tokens:
                    filtered_ids = [
                        pid
                        for pid in candidate_ids
                        if all(token in field_cache[pid] or token in corpus_cache[pid] for token in tokens)
                    ]
                    if filtered_ids:
                        candidate_ids = filtered_ids
                if query_fold:
                    filtered_ids = [
                        pid
                        for pid in candidate_ids
                        if query_fold in field_cache[pid] or query_fold in corpus_cache[pid]
                    ]
                    if filtered_ids:
                        candidate_ids = filtered_ids

                if not candidate_ids and tokens:
                    candidate_ids = [
                        pid
                        for pid in self.property_order
                        if all(
                            token in self.search_corpus.get(pid, "").casefold()
                            for token in tokens
                        )
                    ]
    
            if not candidate_ids:
                substring = trimmed.casefold()
                filtered: List[str] = []
                for pid in self.property_order:
                    field_value = _field_text(pid)
                    corpus_value = self.search_corpus.get(pid, '').casefold()
                    if substring and (substring in field_value or substring in corpus_value):
                        filtered.append(pid)
                        continue
                    if tokens and all((token in field_value) or (token in corpus_value) for token in tokens):
                        filtered.append(pid)
                candidate_ids = filtered

            if not candidate_ids:
                return [], []

    
            regions = {self._canonical(value) for value in filters.get("regions", []) if value}
            vacancy_filter = filters.get("vacancy")
            units_min = filters.get("units_min")
            units_max = filters.get("units_max")
    
            results: List[Dict[str, Any]] = []
            employee_matches: List[Dict[str, Any]] = []
            seen_employee_keys: set[Tuple[str, str]] = set()
    
            for property_id in candidate_ids:
                record = self.properties_payload.get(property_id)
                if not record:
                    continue
                if regions:
                    region_value = self._canonical(record.get("region") or "")
                    if region_value not in regions:
                        continue
                if vacancy_filter == "with" and not record.get("hasVacancy"):
                    continue
                if vacancy_filter == "without" and record.get("hasVacancy"):
                    continue
                units_value = record.get("units")
                if units_min is not None and units_value is not None and units_value < units_min:
                    continue
                if units_max is not None and units_value is not None and units_value > units_max:
                    continue
    
                results.append(self._copy_property(property_id))
    
                if trimmed:
                    matches = employee_match_cache.get(property_id)
                    if matches is None:
                        matches = self._collect_employee_matches(property_id, trimmed)
                        employee_match_cache[property_id] = matches
                    for match in matches:
                        key = (
                            match.get("propertyId", ""),
                            (match.get("employeeId") or match.get("employeeName") or ""),
                        )
                        if key in seen_employee_keys:
                            continue
                        seen_employee_keys.add(key)
                        employee_matches.append(match)
                        if len(employee_matches) >= self.MAX_EMPLOYEE_MATCHES:
                            break
                if len(employee_matches) >= self.MAX_EMPLOYEE_MATCHES:
                    break
    
            return results, employee_matches
    def get_employees_for_property(
        self, identifier: str
    ) -> Optional[Dict[str, Any]]:
        with self.lock:
            property_id = self._resolve_property_identifier(identifier)
            if not property_id:
                return None
            property_record = self.properties_payload.get(property_id)
            if not property_record:
                return None
            employees = []
            for position in self.positions_by_property.get(property_id, []):
                employees.append(
                    {
                        "employeeId": position.get("employeeId"),
                        "employeeName": position.get("employeeName"),
                        "jobTitle": position.get("jobTitle"),
                        "isVacant": position.get("isVacant", False),
                        "email": position.get("email"),
                        "phone": position.get("phone"),
                    }
                )
            return {
                "property": property_record.get("property"),
                "propertyId": property_id,
                "hasVacancy": property_record.get("hasVacancy", False),
                "employees": employees,
            }

    def _copy_property(self, property_id: str) -> Dict[str, Any]:
        record = self.properties_payload[property_id]
        return {key: value for key, value in record.items()}

    def _resolve_property_identifier(self, identifier: str) -> Optional[str]:
        if not identifier:
            return None
        identifier_clean = self._canonical(identifier)
        if identifier_clean in (
            self._canonical(pid) for pid in self.properties_payload.keys()
        ):
            for property_id in self.properties_payload.keys():
                if self._canonical(property_id) == identifier_clean:
                    return property_id
        return self.name_to_property_id.get(identifier_clean)

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Missing configuration file: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as config_file:
            return yaml.safe_load(config_file) or {}

    def _read_excel(self, filename: str) -> pd.DataFrame:
        path = self.data_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing Excel file: {path}")
        return pd.read_excel(path, engine="openpyxl")

    def _normalize_dataframe(
        self, df: pd.DataFrame, mapping: Dict[str, Iterable[str]]
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=mapping.keys())
        normalized_columns = {}
        canonical_columns = {self._canonical(col): col for col in df.columns}
        for target, candidates in mapping.items():
            found_column = None
            for candidate in candidates:
                candidate_key = self._canonical(candidate)
                if candidate_key in canonical_columns:
                    found_column = canonical_columns[candidate_key]
                    break
            if found_column is not None:
                normalized_columns[target] = df[found_column]
            else:
                normalized_columns[target] = pd.Series([None] * len(df))
        return pd.DataFrame(normalized_columns)

    def _prepare_employees(
        self, df: pd.DataFrame
    ) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        for row in df.to_dict(orient="records"):
            employee_id = self._clean_string(row.get("EmployeeID"))
            if not employee_id:
                continue
            first_name = self._clean_nullable(row.get("FirstName"))
            last_name = self._clean_nullable(row.get("LastName"))
            employee_name = self._clean_nullable(row.get("EmployeeName"))
            if not employee_name:
                combined = ' '.join(part for part in [first_name, last_name] if part)
                employee_name = combined or None
            record = {
                "employeeId": employee_id,
                "employeeName": employee_name,
                "firstName": first_name,
                "lastName": last_name,
                "email": self._clean_nullable(row.get("Email")),
                "phone": self._clean_nullable(row.get("Phone")),
            }
            lookup[self._canonical(employee_id)] = record
        return lookup

    def _prepare_properties(
        self, df: pd.DataFrame
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str], Dict[str, str], set[str], Dict[str, int]]:
        properties: Dict[str, Dict[str, Any]] = {}
        property_order: List[str] = []
        name_to_id: Dict[str, str] = {}
        regions: set[str] = set()
        skipped = 0

        for idx, row in enumerate(df.to_dict(orient="records")):
            property_name = self._clean_nullable(row.get("Property"))
            if not property_name:
                skipped += 1
                continue
            property_id = self._clean_nullable(row.get("PropertyID"))
            if property_id:
                property_id = property_id.strip()
            else:
                property_id = self._generate_property_id(property_name)

            address = self._clean_nullable(row.get("Address"))
            city = self._clean_nullable(row.get("City"))
            state = self._clean_nullable(row.get("State"))
            zip_code = self._clean_nullable(row.get("Zip"))
            website = self._clean_nullable(row.get("Website"))
            phone = self._clean_nullable(row.get("Phone"))
            region = self._clean_nullable(row.get("Region"))
            units = self._coerce_int(row.get("Units"))
            latitude = self._coerce_float(row.get("Latitude"))
            longitude = self._coerce_float(row.get("Longitude"))
            has_coordinates = latitude is not None and longitude is not None
            provided_regional_manager = self._assemble_staff_record(
                self._clean_nullable(row.get("RegionalManager")),
                "Regional Manager",
                self._clean_nullable(row.get("RegionalManagerEmail")),
                self._clean_nullable(row.get("RegionalManagerPhone")),
            )
            provided_regional_maintenance = self._assemble_staff_record(
                self._clean_nullable(row.get("RegionalMaintenanceSupervisor")),
                "Regional Maintenance Supervisor",
                self._clean_nullable(row.get("RegionalMaintenanceEmail")),
                self._clean_nullable(row.get("RegionalMaintenancePhone")),
            )

            record = {

                "propertyId": property_id,
                "property": property_name,
                "address": address,
                "city": city,
                "state": state,
                "zip": zip_code,
                "latitude": latitude,
                "longitude": longitude,
                "hasCoordinates": has_coordinates,
                "units": units,
                "region": region,
                "website": website,
                "phone": phone,
                "hasVacancy": False,
                "vacantPositions": 0,
                "totalPositions": 0,
                "markerColor": "green",
                "vacancyLabel": "Fully staffed",
                "regionalManager": provided_regional_manager,
                "regionalMaintenanceSupervisor": provided_regional_maintenance,
                "popupHtml": "",
                "tooltip": "",
            }
            properties[property_id] = record
            property_order.append(property_id)
            name_to_id[self._canonical(property_name)] = property_id
            if region:
                regions.add(region)
        return properties, property_order, name_to_id, regions, {"skipped": skipped}

    def _prepare_positions(
        self,
        df: pd.DataFrame,
        properties: Dict[str, Dict[str, Any]],
        name_to_id: Dict[str, str],
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
        positions_by_property: Dict[str, List[Dict[str, Any]]] = {}
        skipped = 0

        treat_missing_vacant = bool(self.flags.get("treat_missing_positions_as_vacant", False))

        for row in df.to_dict(orient="records"):
            raw_property_id = self._clean_nullable(row.get("PropertyID"))
            property_name = self._clean_nullable(row.get("Property"))
            property_id = None

            if raw_property_id:
                property_id = raw_property_id.strip()
                if property_id not in properties:
                    property_id = None

            if property_id is None and property_name:
                property_id = name_to_id.get(self._canonical(property_name))

            if property_id is None or property_id not in properties:
                skipped += 1
                continue

            employee_id = self._clean_string(row.get("EmployeeID"))
            is_vacant = self._coerce_bool(row.get("IsVacant"))
            if is_vacant is None:
                is_vacant = False
            if treat_missing_vacant and not employee_id:
                is_vacant = True

            employee_record = (
                self.employees_lookup.get(self._canonical(employee_id))
                if employee_id
                else None
            )

            fallback_first = self._clean_nullable(row.get("EmployeeFirstName"))
            fallback_last = self._clean_nullable(row.get("EmployeeLastName"))
            fallback_name = ' '.join(part for part in [fallback_first, fallback_last] if part) or None

            job_title = self._clean_nullable(row.get("JobTitle"))
            is_vacant_flag = bool(is_vacant)

            employee_name = (
                employee_record.get("employeeName")
                if employee_record
                else (fallback_name if not is_vacant_flag else None)
            )

            position_record = {
                "propertyId": property_id,
                "property": properties[property_id]["property"],
                "employeeId": employee_record.get("employeeId") if employee_record else (employee_id or None),
                "employeeName": employee_name,
                "email": employee_record.get("email") if employee_record else None,
                "phone": employee_record.get("phone") if employee_record else None,
                "jobTitle": job_title,
                "isVacant": is_vacant_flag,
            }
            positions_by_property.setdefault(property_id, []).append(position_record)

        for property_id, position_list in positions_by_property.items():
            position_list.sort(
                key=lambda item: (
                    not item.get("isVacant"),
                    (item.get("jobTitle") or "").lower(),
                    (item.get("employeeName") or "").lower(),
                )
            )

        return positions_by_property, {"skipped": skipped}

    def _finalize_properties(
        self,
        properties: Dict[str, Dict[str, Any]],
        positions_by_property: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        for property_id, record in properties.items():
            positions = positions_by_property.get(property_id, [])
            vacant_count = sum(1 for position in positions if position.get("isVacant"))
            total_positions = len(positions)
            has_vacancy = vacant_count > 0
            record["hasVacancy"] = has_vacancy
            record["vacantPositions"] = vacant_count
            record["totalPositions"] = total_positions
            record["markerColor"] = "yellow" if has_vacancy else "green"
            record["vacancyLabel"] = "Vacancy" if has_vacancy else "Fully staffed"
            fallback_manager, fallback_maintenance = self._extract_key_staff(positions)
            record["regionalManager"] = self._merge_staff_entries(record.get("regionalManager"), fallback_manager)
            record["regionalMaintenanceSupervisor"] = self._merge_staff_entries(record.get("regionalMaintenanceSupervisor"), fallback_maintenance)
            record["tooltip"] = self._build_tooltip(record)
            record["popupHtml"] = self._build_popup_html(record)

    def _collect_employee_matches(
        self, property_id: str, query: str
    ) -> List[Dict[str, Any]]:
        property_record = self.properties_payload.get(property_id, {})
        positions = list(self.positions_by_property.get(property_id, []))

        key_staff: List[Dict[str, Any]] = []
        for key, fallback_title in (
            ("regionalManager", "Regional Manager"),
            ("regionalMaintenanceSupervisor", "Regional Maintenance"),
        ):
            staff = property_record.get(key)
            if not staff:
                continue
            staff_entry = {
                "employeeId": staff.get("employeeId"),
                "employeeName": staff.get("employeeName"),
                "jobTitle": staff.get("jobTitle") or fallback_title,
                "isVacant": staff.get("isVacant", False),
                "email": staff.get("email"),
                "phone": staff.get("phone"),
            }
            key_staff.append(staff_entry)

        staff_entries = positions + key_staff
        if not staff_entries:
            return []

        query_fold = query.casefold()
        tokens = [token for token in re.split(r"\s+", query_fold) if token]

        matches: List[Dict[str, Any]] = []
        for entry in staff_entries:
            is_vacant = bool(entry.get("isVacant"))
            job_title = self._clean_nullable(entry.get("jobTitle")) or ""
            employee_name = self._clean_nullable(entry.get("employeeName")) or ""
            if not employee_name and is_vacant and job_title:
                employee_name = f"Vacant - {job_title}"
            elif not employee_name and is_vacant:
                employee_name = "Vacant position"
            elif not employee_name:
                identifier = self._clean_nullable(entry.get("employeeId")) or ""
                employee_name = identifier or (job_title or "Staff member")

            terms = [
                employee_name,
                job_title,
                property_record.get("property") or "",
                property_record.get("city") or "",
                property_record.get("region") or "",
            ]
            if is_vacant:
                terms.append("vacant position")

            haystack = " ".join(term for term in terms if term)
            if not haystack:
                continue

            haystack_fold = haystack.casefold()
            if query_fold not in haystack_fold:
                if tokens and not all(token in haystack_fold for token in tokens):
                    continue
                if not tokens:
                    continue

            score = fuzz.WRatio(query, haystack) if haystack else 0
            match: Dict[str, Any] = {
                "propertyId": property_id,
                "property": property_record.get("property"),
                "city": property_record.get("city"),
                "state": property_record.get("state"),
                "region": property_record.get("region"),
                "employeeId": entry.get("employeeId"),
                "employeeName": employee_name,
                "jobTitle": job_title or None,
                "isVacant": is_vacant,
                "score": score,
            }
            email = self._clean_nullable(entry.get("email"))
            phone = self._clean_nullable(entry.get("phone"))
            if email:
                match["email"] = email
            if phone:
                match["phone"] = phone
            matches.append(match)

        matches.sort(
            key=lambda item: (
                -(item.get("score") or 0),
                item.get("isVacant", False),
                (item.get("employeeName") or "").casefold(),
            )
        )
        return matches[: self.MAX_EMPLOYEE_MATCHES]


    def _extract_key_staff(self, positions: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        regional_manager: Optional[Dict[str, Any]] = None
        regional_maintenance: Optional[Dict[str, Any]] = None
        for position in positions:
            title = (position.get("jobTitle") or "").casefold()
            if not title:
                continue
            if regional_manager is None and "regional" in title and "manager" in title and "maintenance" not in title:
                regional_manager = position
            if regional_maintenance is None and "regional" in title and ("maintenance" in title or "service" in title):
                regional_maintenance = position
        return self._format_staff_member(regional_manager), self._format_staff_member(regional_maintenance)

    @staticmethod
    def _format_staff_member(position: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not position:
            return None
        name = position.get("employeeName") or ("Vacant" if position.get("isVacant", False) else None)
        if not name:
            name = "Vacant"
        return {
            "employeeId": position.get("employeeId"),
            "employeeName": name,
            "jobTitle": position.get("jobTitle"),
            "isVacant": position.get("isVacant", False),
            "email": position.get("email"),
            "phone": position.get("phone"),
        }

    @staticmethod
    def _assemble_staff_record(name: Optional[str], default_title: str, email: Optional[str], phone: Optional[str]) -> Optional[Dict[str, Any]]:
        if not name:
            return None
        return {
            "employeeId": None,
            "employeeName": name,
            "jobTitle": default_title,
            "isVacant": False,
            "email": email,
            "phone": phone,
        }

    @staticmethod
    def _merge_staff_entries(primary: Optional[Dict[str, Any]], fallback: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if primary and primary.get("employeeName"):
            if fallback:
                for key in ("jobTitle", "email", "phone", "employeeId"):
                    if not primary.get(key) and fallback.get(key):
                        primary[key] = fallback[key]
            return primary
        return fallback or primary

    @staticmethod
    def _staff_popup_line(label: str, staff: Optional[Dict[str, Any]]) -> str:
        label_html = html.escape(label)
        if not staff:
            return (
                '<span class="block text-sm text-slate-600">'
                f'<span class="font-semibold text-slate-700">{label_html}:</span> '
                '<span class="text-slate-500">Not assigned</span>'
                '</span>'
            )
        name = html.escape(staff.get("employeeName") or "Not assigned")
        contact_bits = []
        email = staff.get("email")
        if email:
            contact_bits.append(f'<a class="text-indigo-600 underline" href="mailto:{html.escape(email)}">Email</a>')
        phone = staff.get("phone")
        if phone:
            contact_bits.append(f'<a class="text-indigo-600 underline" href="tel:{html.escape(phone)}">Phone</a>')
        contact_html = ''
        if contact_bits:
            joined = ' &middot; '.join(contact_bits)
            contact_html = f'<span class="ml-2 space-x-2 text-xs">{joined}</span>'
        return (
            '<span class="block text-sm text-slate-600">'
            f'<span class="font-semibold text-slate-700">{label_html}:</span> '
            f'{name}{contact_html}'
            '</span>'
        )

    def _build_tooltip(self, record: Dict[str, Any]) -> str:
        city = record.get("city")
        state = record.get("state")
        location = "Unknown"
        if city and state:
            location = f"{city}, {state}"
        elif city:
            location = city
        elif state:
            location = state
        units = record.get("units")
        units_text = f"Units {units}" if units is not None else "Units n/a"
        vacancy_text = "Vacancy" if record.get("hasVacancy") else "Fully staffed"
        return f"{record.get('property')} — {location} · {units_text} · {vacancy_text}"

    def _build_popup_html(self, record: Dict[str, Any]) -> str:
        property_id = record.get("propertyId") or ""
        property_name = record.get("property") or ""
        
        # Build address components like the sidebar card
        city = record.get("city")
        state = record.get("state")
        location = "Location unavailable"
        if city and state:
            location = f"{city}, {state}"
        elif city:
            location = city
        elif state:
            location = state
        
        address_parts = [
            record.get("address"),
            ", ".join(filter(None, [city, state])),
            record.get("zip"),
        ]
        full_address = ", ".join([part for part in address_parts if part]) or "Address n/a"
        
        # Build metadata like the sidebar card
        units = record.get("units")
        units_text = f"{units} units" if units is not None else "Units n/a"
        region_text = f"Region: {record.get('region')}" if record.get('region') else ""
        
        # Vacancy status and details
        vacancy_class = (
            "bg-amber-300/90 text-slate-900" if record.get("hasVacancy") 
            else "bg-emerald-300/90 text-slate-900"
        )
        vacancy_label = "Vacancy" if record.get("hasVacancy") else "Fully staffed"
        vacancy_details = (
            f"{record.get('vacantPositions')} open" if record.get("hasVacancy")
            else "All positions filled"
        )
        
        # No location badge
        no_location_badge = (
            '' if record.get("hasCoordinates")
            else '<span class="inline-block rounded bg-amber-200 px-2 py-1 text-xs font-medium text-amber-800 mt-2">No map location</span>'
        )
        
        # Format staff like sidebar card
        def format_staff_text(staff):
            if not staff:
                return "Not assigned"
            if staff.get("isVacant"):
                return "Vacant"
            return staff.get("employeeName") or "Not assigned"
        
        regional_manager = format_staff_text(record.get("regionalManager"))
        regional_maintenance = format_staff_text(record.get("regionalMaintenanceSupervisor"))
        
        # Build metadata line
        meta_parts = [units_text]
        if region_text:
            meta_parts.append(region_text)
        meta_html = " • ".join(meta_parts)
        
        return (
            f'<div class="bg-slate-900/60 rounded-lg border border-slate-800 p-4 shadow-md min-w-[280px]">'
            f'<div class="flex items-start justify-between gap-3">'
            f'<div class="min-w-0">'
            f'<h3 class="truncate text-base font-semibold text-white">{html.escape(property_name)}</h3>'
            f'<p class="text-xs text-slate-300">{html.escape(location)}</p>'
            f'</div>'
            f'<span class="status-chip {vacancy_class}">{html.escape(vacancy_label)}</span>'
            f'</div>'
            f'<div class="mt-3 space-y-2 text-xs text-slate-300">'
            f'<p>{html.escape(full_address)}</p>'
            f'<div class="text-[11px] uppercase tracking-wide text-slate-400">{html.escape(meta_html)}</div>'
            f'<p>{html.escape(vacancy_details)}</p>'
            f'{no_location_badge}'
            f'</div>'
            f'<div class="mt-3 space-y-1 text-xs text-slate-200">'
            f'<p><span class="font-semibold text-slate-100">Regional Manager:</span> {html.escape(regional_manager)}</p>'
            f'<p><span class="font-semibold text-slate-100">Regional Maintenance:</span> {html.escape(regional_maintenance)}</p>'
            f'</div>'
            f'<div class="mt-4">'
            f'<button type="button" class="view-staff-btn w-full rounded-md bg-indigo-600 px-3 py-2 text-sm font-semibold text-white hover:bg-indigo-700" '
            f'data-property-id="{html.escape(property_id)}" data-property-name="{html.escape(property_name)}">'
            f'View staff</button>'
            f'</div>'
            f'</div>'
        )

    def _build_search_corpus(
        self,
        properties: Dict[str, Dict[str, Any]],
        positions_by_property: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, str]:
        corpus: Dict[str, str] = {}
        for property_id, record in properties.items():
            terms: List[str] = [
                record.get("property") or "",
                record.get("address") or "",
                record.get("city") or "",
                record.get("state") or "",
                record.get("zip") or "",
                record.get("region") or "",
            ]
            for position in positions_by_property.get(property_id, []):
                if position.get("employeeName"):
                    terms.append(position["employeeName"])
                if position.get("jobTitle"):
                    terms.append(position["jobTitle"])
            for key in ("regionalManager", "regionalMaintenanceSupervisor"):
                staff = record.get(key) or {}
                name = staff.get("employeeName")
                title = staff.get("jobTitle")
                if name:
                    terms.append(name)
                if title:
                    terms.append(title)
            text = " ".join(term for term in terms if term)
            corpus[property_id] = text
        return corpus

    @staticmethod
    def _clean_string(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if text.lower() in {"nan", "none"}:
            return ""
        return text

    @staticmethod
    def _clean_nullable(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none"}:
            return None
        return text

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(number):
            return None
        return number

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(number):
            return None
        return int(number)

    @staticmethod
    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return None
            return bool(value)
        text = str(value).strip().lower()
        if not text:
            return None
        if text in {"y", "yes", "true", "1", "vacant", "open"}:
            return True
        if text in {"n", "no", "false", "0", "filled", "closed"}:
            return False
        return None

    @staticmethod
    def _canonical(value: str) -> str:
        return re.sub(r"\s+", "", value or "").casefold()

    @staticmethod
    def _generate_property_id(name: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", name.strip().casefold())
        slug = slug.strip("-") or "property"
        digest = hashlib.sha1(name.strip().casefold().encode("utf-8")).hexdigest()[:6]
        return f"{slug}-{digest}"


DATA_DIR = Path("data")
CONFIG_PATH = Path("config.yaml")
datastore = DataStore(DATA_DIR, CONFIG_PATH)

app = Flask(__name__)
CORS(app)


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.route("/api/properties", methods=["GET"])
def api_properties() -> Any:
    properties = datastore.get_properties()
    return jsonify(properties)


@app.route("/api/employees", methods=["GET"])
def api_employees() -> Any:
    identifier = request.args.get("property", "")
    if not identifier:
        return jsonify({"message": "Query parameter 'property' is required."}), 400
    result = datastore.get_employees_for_property(identifier)
    if result is None:
        return jsonify({"message": "Property not found."}), 404
    return jsonify(result)


@app.route("/api/search", methods=["GET"])
def api_search() -> Any:
    query = request.args.get("q", "")
    regions = request.args.getlist("region")
    vacancy = request.args.get("vacancy")
    vacancy_value = None
    if vacancy in {"with", "without"}:
        vacancy_value = vacancy
    units_min = request.args.get("unitsMin")
    units_max = request.args.get("unitsMax")
    filters = {
        "regions": regions,
        "vacancy": vacancy_value,
        "units_min": _parse_int(units_min),
        "units_max": _parse_int(units_max),
    }
    properties, employee_matches = datastore.search_properties(query, filters)
    return jsonify({
        "properties": properties,
        "employeeMatches": employee_matches,
    })


@app.route("/api/reload", methods=["POST"])
def api_reload() -> Any:
    try:
        stats = datastore.reload()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to reload data: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500
    return jsonify({"status": "ok", "stats": stats})


def _parse_int(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def create_app() -> Flask:
    return app


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1].lower() == "serve":
        from waitress import serve
        logger.info("Starting Waitress server on port 5000")
        serve(app, host="0.0.0.0", port=5000)
    else:
        app.run(debug=True)






