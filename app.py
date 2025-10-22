# -*- coding: utf-8 -*-







# Your existing app, with minimal admin-plumbing added.















from __future__ import annotations















# === ADDED FOR ADMIN ===







import os  # new















import hashlib







import html







import logging







import re







import threading







from pathlib import Path







from typing import Any, Dict, Iterable, List, Optional, Tuple







from urllib.parse import quote_plus
try:
    import requests  # used for auto-geocoding
except Exception:  # pragma: no cover
    requests = None







from datetime import datetime















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







            zip_code = self._normalize_postal_code(row.get("Zip"))







            website = self._clean_nullable(row.get("Website"))







            phone = self._clean_nullable(row.get("Phone"))

            positions_text = self._clean_nullable(row.get("Position(s)"))

            pays_text = self._clean_nullable(row.get("Pay(s)"))

            region = self._clean_nullable(row.get("Region"))







            units = self._coerce_int(row.get("Units"))

            # Attempt auto-geocoding if coordinates are missing and address data is present
            try:
                if (row.get("Latitude") in (None, "")) and (row.get("Longitude") in (None, "")):
                    if (row.get("Address") or row.get("City") or row.get("State") or row.get("Zip") or row.get("ZIP")):
                        # Normalize ZIP key expected by helper
                        if ("ZIP" not in row) and ("Zip" in row):
                            row["ZIP"] = row.get("Zip")
                        _maybe_autogeocode(row)
            except Exception:
                pass

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

                "positions": positions_text,

                "pays": pays_text,

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







            if treat_missing_vacant:
                _fallback_first = self._clean_nullable(row.get("EmployeeFirstName"))
                _fallback_last = self._clean_nullable(row.get("EmployeeLastName"))
                _fallback_name = ' '.join(part for part in [_fallback_first, _fallback_last] if part) or None
                no_identifier = not bool((employee_id or "").strip())
                no_name = not bool((_fallback_name or "").strip())
                if no_identifier and no_name:
                    # Treat missing assignment as vacant for most roles,
                    # but keep key roles (Property Manager/Maintenance Supervisor) as unassigned unless explicitly vacant
                    job_title = self._clean_nullable(row.get("JobTitle")) or self._clean_nullable(row.get("Position Title"))
                    jt = (job_title or "").casefold()
                    is_key_role = (
                        ("manager" in jt and "property" in jt and "regional" not in jt and "assistant" not in jt)
                        or ((("maintenance" in jt and ("supervisor" in jt or "manager" in jt)) or ("service" in jt and "manager" in jt)) and ("regional" not in jt))
                    )
                    is_vacant = False if is_key_role else True















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















            # Determine unassigned flag (no vacancy, no id, no name)
            _eid = (employee_id or "").strip()
            _ename = (str(employee_name) if employee_name is not None else "").strip()
            is_unassigned = (not is_vacant_flag) and (not _eid) and (not _ename)

            position_record = {







                "propertyId": property_id,







                "property": properties[property_id]["property"],







                "employeeId": employee_record.get("employeeId") if employee_record else (employee_id or None),







                "employeeName": employee_name,







                "email": employee_record.get("email") if employee_record else None,







                "phone": employee_record.get("phone") if employee_record else None,







                "jobTitle": job_title,







                "isVacant": is_vacant_flag,
                "isUnassigned": is_unassigned,







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

            # Mark properties with unassigned key roles (Property Manager / Maintenance Supervisor)
            try:
                def _title(txt: Optional[str]) -> str:
                    return (txt or "").casefold().strip()

                def _is_unassigned(pos: Dict[str, Any]) -> bool:
                    # Not explicitly vacant and missing concrete assignment
                    if bool(pos.get("isVacant")):
                        return False
                    has_id = (pos.get("employeeId") or "").strip() != ""
                    has_name = (pos.get("employeeName") or "").strip() != ""
                    return not (has_id or has_name)

                unassigned_pm = False
                unassigned_ms = False
                for pos in positions:
                    t = _title(pos.get("jobTitle"))
                    if not t:
                        continue
                    # Property Manager (exclude regional/assistant)
                    if (
                        "manager" in t and "property" in t and "regional" not in t and "assistant" not in t
                    ):
                        if _is_unassigned(pos):
                            unassigned_pm = True
                    # Maintenance Supervisor / Service Manager (exclude regional)
                    if (
                        ("maintenance" in t and ("supervisor" in t or "manager" in t))
                        or ("service" in t and "manager" in t)
                    ) and ("regional" not in t):
                        if _is_unassigned(pos):
                            unassigned_ms = True

                record["hasUnassignedKeyRoles"] = bool(unassigned_pm or unassigned_ms)
            except Exception:
                # Fail-safe: do not block rendering if classification fails
                record["hasUnassignedKeyRoles"] = False







            record["popupHtml"] = self._build_popup_html(record)

            # Post-process: detect "no information" case for key roles and adjust marker/labels
            try:
                def _title(txt: Optional[str]) -> str:
                    return (txt or "").casefold().strip()

                def _is_unassigned(pos: Dict[str, Any]) -> bool:
                    # Not explicitly vacant and missing concrete assignment
                    if bool(pos.get("isVacant")):
                        return False
                    has_id = (pos.get("employeeId") or "").strip() != ""
                    has_name = (pos.get("employeeName") or "").strip() != ""
                    return not (has_id or has_name)

                seen_pm = False
                seen_ms = False
                any_unassigned_pm = False
                any_unassigned_ms = False

                for pos in positions:
                    t = _title(pos.get("jobTitle"))
                    if not t:
                        continue
                    # Property Manager (exclude regional/assistant)
                    if ("manager" in t and "property" in t and "regional" not in t and "assistant" not in t):
                        seen_pm = True
                        if _is_unassigned(pos):
                            any_unassigned_pm = True
                    # Maintenance Supervisor / Service Manager (exclude regional)
                    if (("maintenance" in t and ("supervisor" in t or "manager" in t)) or ("service" in t and "manager" in t)) and ("regional" not in t):
                        seen_ms = True
                        if _is_unassigned(pos):
                            any_unassigned_ms = True

                unassigned_pm = (not seen_pm) or any_unassigned_pm
                unassigned_ms = (not seen_ms) or any_unassigned_ms

                record["hasUnassignedKeyRoles"] = bool(unassigned_pm or unassigned_ms)

                # If BOTH PM and MS are unassigned (no info), treat property as "no info":
                # - Force non-vacancy for marker coloring, regardless of other vacancies
                if unassigned_pm and unassigned_ms:
                    record["hasNoInfo"] = True
                    record["hasVacancy"] = False
                    record["markerColor"] = "green"
                    record["vacancyLabel"] = "Fully staffed"
                else:
                    record["hasNoInfo"] = False
            except Exception:
                # Fail-safe: do not block rendering if classification fails
                record["hasUnassignedKeyRoles"] = record.get("hasUnassignedKeyRoles", False)
                record["hasNoInfo"] = record.get("hasNoInfo", False)

            # Rebuild tooltip/popup to reflect any overrides
            record["tooltip"] = self._build_tooltip(record)
            record["popupHtml"] = self._build_popup_html(record)
            if record.get("hasNoInfo") and isinstance(record.get("tooltip"), str):
                try:
                    record["tooltip"] = record["tooltip"].replace("Fully staffed", "Info missing")
                except Exception:
                    pass















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















        units = record.get("units")







        units_text = f"{units} units" if units is not None else "Units n/a"







        region_text = f"Region: {record.get('region')}" if record.get('region') else ""















        vacancy_class = (
            "bg-sky-300/90 text-slate-900" if record.get("hasNoInfo")
            else ("bg-amber-300/90 text-slate-900" if record.get("hasVacancy") else "bg-emerald-300/90 text-slate-900")
        )







        vacancy_label = (
            "Info missing" if record.get("hasNoInfo")
            else ("Vacancy" if record.get("hasVacancy") else "Fully staffed")
        )







        vacancy_details = (
            "Key roles unknown" if record.get("hasNoInfo")
            else (f"{record.get('vacantPositions')} open" if record.get("hasVacancy") else "All positions filled")
        )















        no_location_badge = (







            '' if record.get("hasCoordinates")







            else '<span class="inline-block rounded bg-amber-200 px-2 py-1 text-xs font-medium text-amber-800 mt-2">No map location</span>'







        )















        def format_staff_text(staff):







            if not staff:







                return "Not assigned"







            if staff.get("isVacant"):
                
                
                
                
                
                
                
                return "Vacant"
            if staff.get("isUnassigned"):
                return "Unassigned"







            return staff.get("employeeName") or "Not assigned"















        regional_manager = format_staff_text(record.get("regionalManager"))







        regional_maintenance = format_staff_text(record.get("regionalMaintenanceSupervisor"))















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







    def _normalize_postal_code(value: Any) -> Optional[str]:







        if value is None:







            return None







        if isinstance(value, int):







            return str(value)







        if isinstance(value, float):







            if pd.isna(value):







                return None







            if value.is_integer():







                return str(int(value))







            text_value = str(value).strip()







        else:







            text_value = str(value).strip()







        if not text_value:







            return None







        cleaned = text_value.replace(' ', '')







        if '.' in cleaned and cleaned.replace('.', '').isdigit():







            head, tail = cleaned.split('.', 1)







            if set(tail) <= {"0"}:







                cleaned = head







        return cleaned















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















# === ADDED FOR ADMIN ===







ADMIN_SECRET = (os.environ.get("ADMIN_SECRET") or "").strip()
# Fallback to config.yaml if no env var
if not ADMIN_SECRET:
    try:
        with open("config.yaml", "r", encoding="utf-8") as _f:
            _cfg = yaml.safe_load(_f) or {}
            ADMIN_SECRET = (
                str(
                    (_cfg.get("admin_secret")
                     or _cfg.get("ADMIN_SECRET")
                     or (_cfg.get("flags", {}) or {}).get("admin_secret")
                     or "")
                ).strip()
            )
    except Exception:
        ADMIN_SECRET = ""
if not ADMIN_SECRET:
    ADMIN_SECRET = "letmein123"















def _supplied_admin_secret() -> str:







    # Header takes precedence; query string is fallback







    value = (request.headers.get("X-Admin-Secret") or request.args.get("admin_secret") or "").strip()
    if not value:
        auth = request.headers.get("Authorization") or ""
        if auth.lower().startswith("bearer "):
            value = auth[7:].strip()
    return value















def _require_admin():







    # Allow bypass via config flag if explicitly disabled
    try:
        with open("config.yaml", "r", encoding="utf-8") as _f:
            _cfg = yaml.safe_load(_f) or {}
            flags = _cfg.get("flags", {}) or {}
            if bool(flags.get("disable_admin_auth")):
                return None
    except Exception:
        pass

    if _supplied_admin_secret() != ADMIN_SECRET:







        return jsonify({"ok": False, "error": "Unauthorized"}), 401







    return None















PROPERTY_FILE_PATH = DATA_DIR / "Properties_geocoded.xlsx"







EMPLOYEE_FILE_PATH = DATA_DIR / "Employee.xlsx"







POSITIONS_FILE_PATH = DATA_DIR / "Positions.xlsx"







TERMINATED_LOG_PATH = DATA_DIR / "Terminated_Employees.xlsx"















PROPERTY_FIELD_MAP = {







    "propertyId": "PropertyID",







    "property": "PropertyName",







    "address": "Address",







    "city": "City",







    "state": "State",







    "zip": "ZIP",







    "units": "Unit Count",







    "region": "Region",







    "phone": "Phone",







    "website": "Website",







    "latitude": "Latitude",







    "longitude": "Longitude",







    "regionalManager": "Regional Manager",







    "regionalMaintenanceSupervisor": "Regional Maintenance Supervisor",





















}







PROPERTY_COLUMN_ORDER = [







    "PropertyID",







    "PropertyName",







    "Address",







    "City",







    "State",







    "ZIP",







    "Unit Count",







    "Region",







    "Phone",







    "Website",







    "Regional Manager",







    "Regional Maintenance Supervisor",








    "Latitude",







    "Longitude",







]















EMPLOYEE_FIELD_MAP = {
    "employeeId": "EmployeeID",
    "firstName": "First Name",
    "lastName": "Last Name",
    "phone": "Phone",
    "email": "Email",
}

EMPLOYEE_COLUMN_ORDER = [
    "EmployeeID",
    "First Name",
    "Last Name",
    "Phone",
    "Email",
]















POSITIONS_LOG_SCHEMAS = {







    "transfers": {







        "sheet": "Transfers",







        "columns": [







            "Timestamp",







            "Effective Date",







            "EmployeeID",







            "EmployeeName",







            "FromPropertyID",







            "FromPropertyName",







            "ToPropertyID",







            "ToPropertyName",







            "Notes",







            "EnteredBy",







        ],







        "field_map": {







            "Effective Date": "effectiveDate",







            "EmployeeID": "employeeId",







            "EmployeeName": "employeeName",







            "FromPropertyID": "fromPropertyId",







            "FromPropertyName": "fromPropertyName",







            "ToPropertyID": "toPropertyId",







            "ToPropertyName": "toPropertyName",







            "Notes": "notes",







            "EnteredBy": "enteredBy",







        },







    },







    "hires": {







        "sheet": "Hires",







        "columns": [







            "Timestamp",







            "Effective Date",







            "EmployeeID",







            "EmployeeName",







            "PropertyID",







            "PropertyName",







            "JobTitle",







            "Notes",







            "EnteredBy",







        ],







        "field_map": {







            "Effective Date": "effectiveDate",







            "EmployeeID": "employeeId",







            "EmployeeName": "employeeName",







            "PropertyID": "propertyId",







            "PropertyName": "propertyName",







            "JobTitle": "jobTitle",







            "Notes": "notes",







            "EnteredBy": "enteredBy",







        },







    },







    "terminations": {







        "sheet": "Terminations",







        "columns": [







            "Timestamp",







            "Effective Date",







            "EmployeeID",







            "EmployeeName",







            "PropertyID",







            "PropertyName",







            "JobTitle",







            "Notes",







            "EnteredBy",







        ],







        "field_map": {







            "Effective Date": "effectiveDate",







            "EmployeeID": "employeeId",







            "EmployeeName": "employeeName",







            "PropertyID": "propertyId",







            "PropertyName": "propertyName",







            "JobTitle": "jobTitle",







            "Notes": "notes",







            "EnteredBy": "enteredBy",







        },







    },







}















def _normalize_text(value: Any) -> Optional[str]:







    if value is None:







        return None







    text = str(value).strip()







    return text or None















def _reorder_columns(df: pd.DataFrame, preferred_order: List[str]) -> pd.DataFrame:







    existing = [col for col in preferred_order if col in df.columns]







    remainder = [col for col in df.columns if col not in existing]







    return df[existing + remainder]















def _read_single_sheet(path: Path) -> pd.DataFrame:







    if not path.exists():







        raise FileNotFoundError(f"Missing Excel file: {path}")







    return pd.read_excel(path, engine="openpyxl")















def _write_single_sheet(path: Path, df: pd.DataFrame) -> None:
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
    except PermissionError as exc:
        raise PermissionError(f"Unable to write to {path}. Close the file and try again.") from exc

def _write_workbook(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for name, sheet_df in sheets.items():
                sheet_df.to_excel(writer, sheet_name=name, index=False)
    except PermissionError as exc:
        raise PermissionError(f"Unable to write to {path}. Close the file and try again.") from exc

@app.errorhandler(PermissionError)
def handle_permission_error(exc: PermissionError):
    message = str(exc) or "Unable to write to the Excel workbook. Close the file and try again."
    return jsonify({"ok": False, "error": message}), 423









def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:







    for column in columns:







        if column not in df.columns:







            df[column] = pd.NA







    return df


def _parse_positions_input(text: Optional[str]) -> List[str]:
    """Parse a free-text positions field into a list of titles.
    Splits on newlines, commas, and semicolons, trims blanks, de-duplicates while preserving order.
    """
    if not text:
        return []
    raw = str(text).replace("\r", "\n")
    parts: List[str] = []
    for chunk in raw.split("\n"):
        for piece in chunk.split(","):
            for leaf in piece.split(";"):
                title = (leaf or "").strip()
                if title:
                    parts.append(title)
    seen = set()
    result: List[str] = []
    for p in parts:
        key = p.casefold()
        if key not in seen:
            seen.add(key)
            result.append(p)
    return result


def _append_positions_for_property(property_id: str, property_name: Optional[str], titles: List[str]) -> int:
    """Append one Positions row per title for the given property.
    Only writes to columns that already exist in the Positions sheet to avoid creating new columns.
    Returns the number of rows appended.
    """
    if not titles:
        return 0
    try:
        sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")
    except FileNotFoundError as exc:
        raise ValueError("Positions workbook is missing") from exc
    positions_df = sheets.get("Positions")
    if positions_df is None:
        raise ValueError("Positions worksheet is missing")

    # Determine allowed columns and simple PositionID generation if present
    allowed_cols = list(positions_df.columns)
    has_col = lambda c: c in allowed_cols

    next_position_id: Optional[int] = None
    id_prefix: Optional[str] = None
    id_width: int = 0
    if has_col("PositionID"):
        # Extract numeric maximum and detect a common prefix/width
        existing = positions_df.get("PositionID").dropna().astype(str).tolist()
        max_num = 0
        for val in existing:
            s = str(val).strip()
            digits = "".join(ch for ch in s if ch.isdigit())
            if digits.isdigit():
                try:
                    n = int(digits)
                    max_num = max(max_num, n)
                    id_width = max(id_width, len(digits))
                    if id_prefix is None and len(digits) < len(s):
                        id_prefix = s.replace(digits, "")
                except Exception:
                    pass
        next_position_id = max_num + 1
        if id_prefix is None:
            id_prefix = ""

    new_rows: List[Dict[str, Any]] = []
    for title in titles:
        row: Dict[str, Any] = {}
        if has_col("PropertyID"):
            row["PropertyID"] = property_id
        if has_col("Property"):
            row["Property"] = property_name or None
        if has_col("Position Title"):
            row["Position Title"] = title
        # Leave employee fields blank
        for col in ("EmployeeID", "Employee First Name", "Employee Last Name"):
            if has_col(col):
                row[col] = ""
        # Assign a PositionID if the column exists
        if has_col("PositionID") and next_position_id is not None:
            if id_width > 0:
                row["PositionID"] = f"{id_prefix}{str(next_position_id).zfill(id_width)}"
            else:
                row["PositionID"] = f"{id_prefix}{next_position_id}"
            next_position_id += 1
        # Only include columns that already exist to avoid creating new ones
        new_rows.append(row)

    if not new_rows:
        return 0

    append_df = pd.DataFrame(new_rows)
    # Ensure we do not add new columns by restricting to existing columns
    append_df = append_df[[c for c in append_df.columns if c in allowed_cols]]
    positions_df = pd.concat([positions_df, append_df], ignore_index=True)
    sheets["Positions"] = positions_df
    _write_workbook(POSITIONS_FILE_PATH, sheets)
    return len(new_rows)


def _place_employee_at_property(
    to_property_id: Optional[str],
    to_property_name: Optional[str],
    position_title: str,
    employee_id: Optional[str],
    employee_name: Optional[str],
    *,
    confirm_replace: bool = True,
    remove_from_source: bool = True,
) -> Dict[str, Any]:
    if not (to_property_id or to_property_name):
        raise ValueError("Destination property is required")
    if not position_title:
        raise ValueError("Position title is required")
    if not (employee_id or employee_name):
        raise ValueError("Employee selection is required")

    try:
        sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")
    except FileNotFoundError as exc:
        raise ValueError("Positions workbook is missing") from exc

    positions_df = sheets.get("Positions")
    if positions_df is None:
        raise ValueError("Positions worksheet is missing")

    def _canonical_series(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).map(datastore._canonical)

    # Resolve destination candidates
    dest_mask = pd.Series([False] * len(positions_df))
    if to_property_id:
        dest_mask = dest_mask | (_canonical_series(positions_df.get("PropertyID", pd.Series(["" ] * len(positions_df)))) == datastore._canonical(to_property_id))
    if to_property_name:
        dest_mask = dest_mask | (_canonical_series(positions_df.get("Property", pd.Series(["" ] * len(positions_df)))) == datastore._canonical(to_property_name))
    candidates = positions_df[dest_mask]
    if not candidates.empty and "Position Title" in candidates.columns:
        title_mask = _canonical_series(candidates["Position Title"]) == datastore._canonical(position_title)
        candidates = candidates[title_mask]

    # If no candidate row exists for this position, create one and reload
    if candidates.empty:
        _append_positions_for_property(str(to_property_id or ""), to_property_name, [position_title])
        try:
            sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")
            positions_df = sheets.get("Positions")
            dest_mask = pd.Series([False] * len(positions_df))
            if to_property_id:
                dest_mask = dest_mask | (_canonical_series(positions_df.get("PropertyID", pd.Series(["" ] * len(positions_df)))) == datastore._canonical(to_property_id))
            if to_property_name:
                dest_mask = dest_mask | (_canonical_series(positions_df.get("Property", pd.Series(["" ] * len(positions_df)))) == datastore._canonical(to_property_name))
            candidates = positions_df[dest_mask]
            if "Position Title" in candidates.columns:
                title_mask = _canonical_series(candidates["Position Title"]) == datastore._canonical(position_title)
                candidates = candidates[title_mask]
        except Exception:
            pass

    if candidates.empty:
        raise ValueError("No matching position found at destination property")

    # Choose destination index
    vacancy_mask = candidates.get("EmployeeID", pd.Series(["" ] * len(candidates))).fillna("").astype(str).str.strip() == ""
    if vacancy_mask.any():
        destination_index = candidates[vacancy_mask].index[0]
    else:
        if not confirm_replace:
            return {"status": "needs-confirmation", "reason": "Position filled"}
        destination_index = candidates.index[0]
        # Clear occupant for this row
        for column in ["EmployeeID", "Employee First Name", "Employee Last Name"]:
            if column in positions_df.columns:
                positions_df.at[destination_index, column] = ""

    # Fill destination row
    first_name = None
    last_name = None
    if employee_name and not employee_id:
        parts = employee_name.split()
        if parts:
            first_name = parts[0]
            if len(parts) > 1:
                last_name = parts[-1]
    if employee_id and "EmployeeID" in positions_df.columns:
        positions_df.at[destination_index, "EmployeeID"] = employee_id
    if "Employee First Name" in positions_df.columns:
        positions_df.at[destination_index, "Employee First Name"] = first_name or ""
    if "Employee Last Name" in positions_df.columns:
        positions_df.at[destination_index, "Employee Last Name"] = last_name or ""
    if to_property_name and "Property" in positions_df.columns:
        positions_df.at[destination_index, "Property"] = to_property_name
    if to_property_id and "PropertyID" in positions_df.columns:
        positions_df.at[destination_index, "PropertyID"] = to_property_id
    if position_title and "Position Title" in positions_df.columns:
        positions_df.at[destination_index, "Position Title"] = position_title

    # Optionally remove from other positions for this employee
    if remove_from_source and employee_id and "EmployeeID" in positions_df.columns:
        mask_other = (_canonical_series(positions_df["EmployeeID"]) == datastore._canonical(employee_id)) & (positions_df.index != destination_index)
        for column in ["EmployeeID", "Employee First Name", "Employee Last Name"]:
            if column in positions_df.columns:
                positions_df.loc[mask_other, column] = ""

    sheets["Positions"] = positions_df
    _write_workbook(POSITIONS_FILE_PATH, sheets)

    return {"destinationIndex": int(destination_index)}


def _upsert_employee_core(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Create or update an employee without duplicating personal records.

    Rules:
    - Only personal info (ID, first, last, email, phone) is persisted in Employee.xlsx
    - If EmployeeID provided, update that row
    - Else, try to find an existing row by first/last and optional email/phone
    - Else, reuse a blank row ID if available; otherwise allocate next ID
    """
    try:
        df = _read_single_sheet(EMPLOYEE_FILE_PATH)
    except FileNotFoundError:
        df = pd.DataFrame(columns=EMPLOYEE_COLUMN_ORDER)
    df = _ensure_columns(df, EMPLOYEE_COLUMN_ORDER)
    df = _reorder_columns(df, EMPLOYEE_COLUMN_ORDER)

    id_column = EMPLOYEE_FIELD_MAP["employeeId"]
    blanks = _find_blank_rows(df, id_column, exclude={id_column})

    employee_id = _normalize_text(payload.get("employeeId"))
    first_name = _normalize_text(payload.get("firstName"))
    last_name = _normalize_text(payload.get("lastName"))
    email_val = _normalize_text(payload.get("email"))
    phone_val = _normalize_text(payload.get("phone"))

    row_index = None
    if employee_id:
        canonical = datastore._canonical(employee_id)
        mask = df[id_column].fillna("").astype(str).map(datastore._canonical) == canonical
        if mask.any():
            row_index = df[mask].index[0]
            employee_id = _clean_identifier(df.at[row_index, id_column]) or employee_id
        else:
            employee_id = None

    # If no explicit ID match, deduplicate by name + optional contact info
    if employee_id is None and (first_name or last_name):
        try:
            fn_col = EMPLOYEE_FIELD_MAP.get("firstName")
            ln_col = EMPLOYEE_FIELD_MAP.get("lastName")
            em_col = EMPLOYEE_FIELD_MAP.get("email")
            ph_col = EMPLOYEE_FIELD_MAP.get("phone")

            def _norm_series(series):
                return series.fillna("").astype(str).map(lambda v: (_normalize_text(v) or "").casefold())

            mask = pd.Series([True] * len(df))
            if first_name:
                mask = mask & (_norm_series(df[fn_col]) == (first_name or "").casefold())
            if last_name:
                mask = mask & (_norm_series(df[ln_col]) == (last_name or "").casefold())
            if email_val:
                mask = mask & (_norm_series(df[em_col]) == (email_val or "").casefold())
            if phone_val:
                mask = mask & (_norm_series(df[ph_col]) == (phone_val or "").casefold())
            if mask.any():
                row_index = df[mask].index[0]
                employee_id = _clean_identifier(df.at[row_index, id_column]) or None
        except Exception:
            pass

    if employee_id is None:
        if blanks:
            row_index, employee_id = blanks[0]
        else:
            employee_id = _allocate_employee_id(df[id_column])

    payload = {**payload, "employeeId": employee_id}
    row_data = _employee_payload_to_row(payload)

    if row_index is not None and isinstance(row_index, int):
        for column, value in row_data.items():
            df.at[row_index, column] = value
    else:
        df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)

    _write_single_sheet(EMPLOYEE_FILE_PATH, df)

    return {
        "employeeId": employee_id,
        "firstName": first_name,
        "lastName": last_name,
        "employeeName": (" ".join([first_name or "", last_name or ""]).strip() or None),
    }
def _property_payload_to_row(payload: Dict[str, Any]) -> Dict[str, Any]:







    row = {}







    for key, column in PROPERTY_FIELD_MAP.items():







        value = payload.get(key)







        if key == "units":







            row[column] = datastore._coerce_int(value)







        elif key in {"latitude", "longitude"}:







            row[column] = datastore._coerce_float(value)







        else:







            row[column] = _normalize_text(value)







    return row















def _employee_payload_to_row(payload: Dict[str, Any]) -> Dict[str, Any]:







    row = {}







    for key, column in EMPLOYEE_FIELD_MAP.items():







        value = payload.get(key)







        if key == "employeeId":







            cleaned = _normalize_text(value)







            if not cleaned:







                raise ValueError("Employee ID is required")







            row[column] = cleaned







        else:







            row[column] = _normalize_text(value)







    return row
















def _load_employees() -> List[Dict[str, Any]]:
    df = _read_single_sheet(EMPLOYEE_FILE_PATH)
    df = _ensure_columns(df, EMPLOYEE_COLUMN_ORDER)
    df = _reorder_columns(df, EMPLOYEE_COLUMN_ORDER)

    records: List[Dict[str, Any]] = []
    for item in df.fillna("").to_dict(orient="records"):
        first = _normalize_text(item.get("First Name")) or ""
        last = _normalize_text(item.get("Last Name")) or ""
        fullname = item.get("Employee Name") or (" ".join([first, last]).strip())
        records.append({
            "employeeId": item.get("EmployeeID", ""),
            "employeeName": fullname,
            "firstName": first,
            "lastName": last,
            "email": item.get("Email", ""),
            "phone": item.get("Phone", ""),
        })
    return records















def _append_log_entry(log_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:







    schema = POSITIONS_LOG_SCHEMAS[log_type]







    row: Dict[str, Any] = {column: None for column in schema["columns"]}







    row["Timestamp"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"







    for column in schema["columns"]:







        if column == "Timestamp":







            continue







        field_key = schema["field_map"].get(column)







        if field_key is None:







            continue







        row[column] = _normalize_text(payload.get(field_key))







    with datastore.lock:







        sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")







        df = sheets.get(schema["sheet"])







        if df is None:







            df = pd.DataFrame(columns=schema["columns"])







        df = _ensure_columns(df, schema["columns"])







        df = _reorder_columns(df, schema["columns"])







        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)







        sheets[schema["sheet"]] = df







        _write_workbook(POSITIONS_FILE_PATH, sheets)



    return row















































@app.route("/admin", methods=["GET"])







def admin_page() -> str:







    return render_template("admin.html")























@app.route("/api/admin/ping", methods=["GET"])







def admin_ping():







    guard = _require_admin()







    if guard:







        return guard







    return jsonify({"ok": True, "message": "admin unlocked"})























@app.route("/api/admin/unlock", methods=["POST"])







def admin_unlock():







    data = request.get_json(silent=True) or {}







    typed = (data.get("key") or "").strip()







    ok = typed == ADMIN_SECRET







    return jsonify({"ok": ok})























@app.route("/api/admin/properties", methods=["GET"])







def admin_list_properties():







    guard = _require_admin()







    if guard:







        return guard







    properties = datastore.get_properties()







    return jsonify({"ok": True, "properties": properties, "regions": datastore.get_regions()})























@app.route("/api/admin/properties", methods=["POST"])







def admin_upsert_property():







    guard = _require_admin()







    if guard:







        return guard







    payload = request.get_json(silent=True) or {}







    property_name = _normalize_text(payload.get("property"))







    if not property_name:







        return jsonify({"ok": False, "error": "Property name is required"}), 400















    requested_id = _normalize_text(payload.get("propertyId"))







    payload["property"] = property_name















    with datastore.lock:







        df = _read_single_sheet(PROPERTY_FILE_PATH)







        df = _ensure_columns(df, PROPERTY_COLUMN_ORDER)







        df = _reorder_columns(df, PROPERTY_COLUMN_ORDER)







        id_column = PROPERTY_FIELD_MAP["propertyId"]







        name_column = PROPERTY_FIELD_MAP["property"]















        blanks = _find_blank_rows(df, id_column, exclude={id_column})







        row_index = None







        property_id = None















        if requested_id:







            canonical_requested = datastore._canonical(requested_id)







            mask = df[id_column].fillna("").astype(str).map(datastore._canonical) == canonical_requested







            if mask.any():







                row_index = df[mask].index[0]







                property_id = _clean_identifier(df.at[row_index, id_column]) or requested_id







            else:







                property_id = requested_id















        if property_id is None:







            if blanks:







                row_index, property_id = blanks[0]







            else:







                property_id = _allocate_property_id(df[id_column])















        canonical_id = datastore._canonical(str(property_id))

        # Ensure PropertyID uniqueness: if duplicates exist, consolidate to a single row
        try:
            dupe_mask = df[id_column].fillna("").astype(str).map(datastore._canonical) == canonical_id
            if dupe_mask.any():
                dupe_indices = list(df[dupe_mask].index)
                # Prefer a non-blank row if present; otherwise the first duplicate
                def _row_is_blank(idx: int) -> bool:
                    row = df.loc[idx]
                    for col in df.columns:
                        if col == id_column:
                            continue
                        val = row.get(col)
                        if isinstance(val, float):
                            try:
                                import math
                                if math.isnan(val):
                                    continue
                            except Exception:
                                pass
                        if str(val).strip():
                            return False
                    return True

                nonblank = [i for i in dupe_indices if not _row_is_blank(i)]
                if row_index is None:
                    row_index = (nonblank[0] if nonblank else dupe_indices[0])
                keep_index = row_index
                extras = [i for i in dupe_indices if i != keep_index]
                if extras:
                    df = df.drop(index=extras).reset_index(drop=True)
        except Exception:
            # Do not fail the transaction for cleanup issues; continue with write
            pass







        row_payload = {**payload, "propertyId": property_id}







        row_data = _property_payload_to_row(row_payload)







        row_data[id_column] = property_id







        row_data[name_column] = property_name















        if row_index is not None and isinstance(row_index, int):
            
            
            
            
            
            
            
            for column, value in row_data.items():







                df.at[row_index, column] = value







        else:
            
            
            
            
            
            
            
            df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)















        _write_single_sheet(PROPERTY_FILE_PATH, df)

        # Create initial positions for this property if provided in payload
        try:
            positions_text = _normalize_text(payload.get("positions"))
            titles = _parse_positions_input(positions_text)
            if titles:
                _append_positions_for_property(str(property_id), property_name, titles)
        except Exception as exc:
            logger.exception("Failed to append positions for property %s: %s", property_id, exc)

        # Optional: handle positionAssignments for immediate placement (transfer or hire)
        try:
            assignments = payload.get("positionAssignments") or []
            for entry in assignments:
                try:
                    position_title = _normalize_text(entry.get("position") or entry.get("positionTitle"))
                    if not position_title:
                        continue
                    action = (_normalize_text(entry.get("action")) or "transfer").casefold()
                    keep_existing = bool(entry.get("keepExisting"))
                    remove_from_source = not keep_existing
                    if action == "transfer":
                        emp_id = _normalize_text(entry.get("employeeId"))
                        emp_name = _normalize_text(entry.get("employeeName"))
                        _place_employee_at_property(str(property_id), property_name, position_title, emp_id, emp_name, confirm_replace=True, remove_from_source=remove_from_source)
                    elif action == "hire":
                        emp_payload = {
                            "employeeId": _normalize_text(entry.get("employeeId")),
                            "firstName": _normalize_text(entry.get("firstName")),
                            "lastName": _normalize_text(entry.get("lastName")),
                            "title": _normalize_text(entry.get("title")) or position_title,
                            "phone": _normalize_text(entry.get("phone")),
                            "email": _normalize_text(entry.get("email")),
                            "property": property_name,
                        }
                        created = _upsert_employee_core(emp_payload)
                        emp_id = created.get("employeeId")
                        emp_name = created.get("employeeName")
                        _place_employee_at_property(str(property_id), property_name, position_title, emp_id, emp_name, confirm_replace=True, remove_from_source=False)
                except Exception as inner:
                    logger.exception("Assignment failed for property %s: %s", property_id, inner)
        except Exception as exc:
            logger.exception("Assignments processing failed for property %s: %s", property_id, exc)















    datastore.reload()







    updated_property = next(







        (item for item in datastore.get_properties() if datastore._canonical(item.get("propertyId")) == canonical_id),







        None,







    )







    return jsonify({"ok": True, "property": updated_property, "propertyId": property_id})

@app.route("/api/admin/geocode", methods=["POST"])
def admin_geocode():
    guard = _require_admin()
    if guard:
        return guard
    payload = request.get_json(silent=True) or {}
    address = _normalize_text(payload.get("address"))
    city = _normalize_text(payload.get("city"))
    state = _normalize_text(payload.get("state"))
    zip_code = _normalize_text(payload.get("zip"))
    bits = [address, city, state, zip_code, "USA"]
    query = ", ".join([x for x in bits if x])
    if not query:
        return jsonify({"ok": False, "error": "Address, city/state or zip required"}), 400
    email = _config_value(["geocode_email"]) or _config_value(["flags", "geocode_email"]) or _config_value(["email"]) or None
    lat, lon, status = _nominatim_geocode(query, email)
    if lat is None or lon is None:
        return jsonify({"ok": False, "error": f"Geocode failed: {status}"}), 502
    return jsonify({"ok": True, "latitude": lat, "longitude": lon, "status": status})























@app.route("/api/admin/properties", methods=["DELETE"])







def admin_remove_property():







    guard = _require_admin()







    if guard:







        return guard







    property_name = _normalize_text(request.args.get("Name") or request.args.get("name"))







    property_id = _normalize_text(







        request.args.get("PropertyID")







        or request.args.get("propertyId")







        or request.args.get("id")







    )







    try:







        removed, _ = _remove_property_records(property_id, property_name)







    except ValueError as exc:







        return jsonify({"ok": False, "error": str(exc)}), 400







    if removed == 0:







        return jsonify({"ok": False, "error": "Property not found"}), 404







    return jsonify({"ok": True, "propertyId": property_id, "propertyName": property_name})























@app.route("/api/admin/properties/<path:property_id>", methods=["DELETE"])







def admin_delete_property(property_id: str):







    guard = _require_admin()







    if guard:







        return guard







    payload = request.get_json(silent=True) or {}







    property_name = _normalize_text(payload.get("property"))







    try:







        removed, _ = _remove_property_records(property_id, property_name)







    except ValueError as exc:







        return jsonify({"ok": False, "error": str(exc)}), 400







    if removed == 0:







        return jsonify({"ok": False, "error": "Property not found"}), 404







    return jsonify({"ok": True, "propertyId": property_id})























@app.route("/api/admin/employees", methods=["GET"])







def admin_list_employees():







    guard = _require_admin()







    if guard:







        return guard







    return jsonify({"ok": True, "employees": _load_employees()})


@app.route("/api/admin/reconcile-employee-ids", methods=["POST"])
def admin_reconcile_employee_ids():
    guard = _require_admin()
    if guard:
        return guard

    try:
        sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")
    except FileNotFoundError:
        return jsonify({"ok": False, "error": "Positions workbook is missing"}), 400

    positions_df = sheets.get("Positions")
    if positions_df is None:
        return jsonify({"ok": False, "error": "Positions worksheet is missing"}), 400

    employees = _load_employees()

    def canon(v: Optional[str]) -> str:
        return datastore._canonical(v)

    by_id: Dict[str, Dict[str, Any]] = {}
    by_name_multi: Dict[str, List[Dict[str, Any]]] = {}
    for e in employees:
        e_id = canon(e.get("employeeId"))
        if e_id:
            by_id[e_id] = e
        key = canon(f"{e.get('firstName') or ''} {e.get('lastName') or ''}")
        if key:
            by_name_multi.setdefault(key, []).append(e)

    by_name_unique: Dict[str, Dict[str, Any]] = {k: v[0] for k, v in by_name_multi.items() if len(v) == 1}

    for col in ("EmployeeID", "Employee First Name", "Employee Last Name"):
        if col not in positions_df.columns:
            positions_df[col] = ""

    updates: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []
    examined = 0
    for idx, row in positions_df.iterrows():
        examined += 1
        eid = str(row.get("EmployeeID") or "").strip()
        first = str(row.get("Employee First Name") or "").strip()
        last = str(row.get("Employee Last Name") or "").strip()
        name_key = canon(f"{first} {last}")

        if eid and canon(eid) in by_id:
            if name_key and name_key in by_name_unique:
                target = by_name_unique[name_key]
                if canon(target.get("employeeId")) != canon(eid):
                    positions_df.at[idx, "EmployeeID"] = target.get("employeeId") or ""
                    if not first:
                        positions_df.at[idx, "Employee First Name"] = target.get("firstName") or ""
                    if not last:
                        positions_df.at[idx, "Employee Last Name"] = target.get("lastName") or ""
                    updates.append({
                        "row": int(idx),
                        "position": str(row.get("Position Title") or ""),
                        "property": str(row.get("Property") or ""),
                        "employeeId": target.get("employeeId") or "",
                    })
            continue

        target: Optional[Dict[str, Any]] = by_name_unique.get(name_key)
        if target:
            positions_df.at[idx, "EmployeeID"] = target.get("employeeId") or ""
            if not first:
                positions_df.at[idx, "Employee First Name"] = target.get("firstName") or ""
            if not last:
                positions_df.at[idx, "Employee Last Name"] = target.get("lastName") or ""
            updates.append({
                "row": int(idx),
                "position": str(row.get("Position Title") or ""),
                "property": str(row.get("Property") or ""),
                "employeeId": target.get("employeeId") or "",
            })
        elif name_key:
            conflicts.append({
                "row": int(idx),
                "position": str(row.get("Position Title") or ""),
                "property": str(row.get("Property") or ""),
                "first": first,
                "last": last,
                "candidates": [e.get("employeeId") for e in by_name_multi.get(name_key, [])],
            })

    if updates:
        sheets["Positions"] = positions_df
        try:
            _write_workbook(POSITIONS_FILE_PATH, sheets)
        except PermissionError as exc:
            return handle_permission_error(exc)

    return jsonify({"ok": True, "examined": examined, "updated": len(updates), "conflicts": conflicts[:50]})























@app.route("/api/admin/employees", methods=["POST"])







def admin_upsert_employee():







    guard = _require_admin()







    if guard:







        return guard







    payload = request.get_json(silent=True) or {}







    employee_id = _normalize_text(payload.get("employeeId"))







    first_name = _normalize_text(payload.get("firstName"))







    last_name = _normalize_text(payload.get("lastName"))







    employee_name = _normalize_text(payload.get("employeeName"))







    if not employee_name:







        employee_name = " ".join(part for part in [first_name, last_name] if part) or None







    payload["employeeName"] = employee_name







    payload["firstName"] = first_name







    payload["lastName"] = last_name















    with datastore.lock:







        df = _read_single_sheet(EMPLOYEE_FILE_PATH)







        df = _ensure_columns(df, EMPLOYEE_COLUMN_ORDER)







        df = _reorder_columns(df, EMPLOYEE_COLUMN_ORDER)







        id_column = EMPLOYEE_FIELD_MAP["employeeId"]















        blanks = _find_blank_rows(df, id_column, exclude={id_column})







        row_index = None















        if employee_id:







            canonical_employee = datastore._canonical(employee_id)







            mask = df[id_column].fillna("").astype(str).map(datastore._canonical) == canonical_employee







            if mask.any():







                row_index = df[mask].index[0]







                employee_id = _clean_identifier(df.at[row_index, id_column]) or employee_id







            else:







                employee_id = None















        if employee_id is None:







            if blanks:







                row_index, employee_id = blanks[0]







            else:







                employee_id = _allocate_employee_id(df[id_column])















        payload["employeeId"] = employee_id







        row_data = _employee_payload_to_row(payload)















        if row_index is not None and isinstance(row_index, int):







            for column, value in row_data.items():







                df.at[row_index, column] = value







        else:







            df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)















        _write_single_sheet(EMPLOYEE_FILE_PATH, df)















    datastore.reload()







    canonical_employee = datastore._canonical(employee_id)







    updated_employee = next(







        (item for item in _load_employees() if datastore._canonical(item.get("employeeId")) == canonical_employee),







        None,







    )







    return jsonify({"ok": True, "employee": updated_employee, "employeeId": employee_id})























@app.route("/api/admin/employees", methods=["DELETE"])







def admin_remove_employee():







    guard = _require_admin()







    if guard:







        return guard







    employee_id = _normalize_text(







        request.args.get("EmployeeID")







        or request.args.get("employeeId")







        or request.args.get("id")







    )







    terminated_on = _normalize_text(request.args.get("date") or request.args.get("terminatedOn"))







    try:







        removed, info = _remove_employee_record(employee_id, terminated_on)







    except ValueError as exc:







        return jsonify({"ok": False, "error": str(exc)}), 400







    if removed == 0:







        return jsonify({"ok": False, "error": "Employee not found"}), 404







    return jsonify({"ok": True, "employeeId": employee_id, "termination": info})























@app.route("/api/admin/employees/<path:employee_id>", methods=["DELETE"])







def admin_delete_employee(employee_id: str):







    guard = _require_admin()







    if guard:







        return guard







    terminated_on = _normalize_text(request.args.get("date") or request.args.get("terminatedOn"))







    try:







        removed, info = _remove_employee_record(employee_id, terminated_on)







    except ValueError as exc:







        return jsonify({"ok": False, "error": str(exc)}), 400







    if removed == 0:







        return jsonify({"ok": False, "error": "Employee not found"}), 404







    return jsonify({"ok": True, "employeeId": employee_id, "termination": info})























@app.route("/api/admin/employees/search", methods=["GET"])







def admin_search_employees():







    guard = _require_admin()







    if guard:







        return guard







    query = request.args.get("q", "")







    results = _search_employees_by_name(query, limit=10)







    return jsonify({"ok": True, "results": results})























@app.route("/api/admin/logs/<string:log_type>", methods=["POST"])







def admin_append_log(log_type: str):







    guard = _require_admin()







    if guard:







        return guard







    key = log_type.lower()







    if key not in POSITIONS_LOG_SCHEMAS:







        return jsonify({"ok": False, "error": "Unknown log type"}), 404







    payload = request.get_json(silent=True) or {}







    entry = _append_log_entry(key, payload)







    return jsonify({"ok": True, "logType": key, "entry": entry})























@app.route("/api/admin/transactions", methods=["POST"])







def admin_create_transaction():







    guard = _require_admin()







    if guard:







        return guard







    payload = request.get_json(silent=True) or {}







    type_value = _normalize_text(payload.get("type"))







    if not type_value:







        return jsonify({"ok": False, "error": "Transaction type is required"}), 400







    key = type_value.casefold()







    if key not in POSITIONS_LOG_SCHEMAS:







        return jsonify({"ok": False, "error": "Unknown transaction type"}), 400







    entry = _append_log_entry(key, payload)







    return jsonify({"ok": True, "logType": key, "entry": entry})























@app.route("/api/admin/transfers", methods=["POST"])







def admin_create_transfer():







    guard = _require_admin()







    if guard:







        return guard







    payload = request.get_json(silent=True) or {}







    try:







        result = _perform_transfer(payload)







    except ValueError as exc:







        return jsonify({"ok": False, "error": str(exc)}), 400







    if result.get("status") in {"needs-confirmation", "needs-selection"}:







        return jsonify(result), 409







    return jsonify({"ok": True, **result})































def _clean_identifier(value: Any) -> Optional[str]:







    if value is None:







        return None







    if isinstance(value, float) and pd.isna(value):







        return None







    if pd.isna(value):







        return None







    text = str(value).strip()







    if not text:







        return None







    if text.endswith(".0") and text[:-2].isdigit():







        text = text[:-2]







    return text























def _is_empty_cell(value: Any) -> bool:







    if value is None:







        return True







    if isinstance(value, float) and pd.isna(value):







        return True







    if pd.isna(value):







        return True







    if isinstance(value, str):







        return not value.strip()







    return False























def _find_blank_rows(df: pd.DataFrame, id_column: str, exclude: Optional[set] = None) -> List[Tuple[int, str]]:







    exclude = set(exclude or set())







    blanks: List[Tuple[int, str]] = []







    columns_to_check = [column for column in df.columns if column not in exclude]







    for idx, row in df.iterrows():







        identifier = _clean_identifier(row.get(id_column))







        if not identifier:







            continue







        if all(_is_empty_cell(row.get(column)) for column in columns_to_check):







            blanks.append((idx, identifier))







    return blanks























def _allocate_property_id(series: pd.Series) -> str:







    highest = 0







    for value in series:







        identifier = _clean_identifier(value)







        if not identifier:







            continue







        match = re.search(r"\d+", identifier)







        if match:







            highest = max(highest, int(match.group()))







    return str(highest + 1 if highest else 1)























def _allocate_employee_id(series: pd.Series) -> str:







    highest = 0







    for value in series:







        identifier = _clean_identifier(value)







        if not identifier:







            continue







        match = re.search(r"(\d+)$", identifier)







        if match:







            highest = max(highest, int(match.group(1)))







    return f"E{highest + 1:03d}" if highest else "E001"


# --- Auto Geocoding helpers ---
def _config_value(path: List[str], default: Optional[str] = None) -> Optional[str]:
    try:
        with open("config.yaml", "r", encoding="utf-8") as _f:
            cfg = yaml.safe_load(_f) or {}
        cur: Any = cfg
        for key in path:
            cur = (cur or {}).get(key)
            if cur is None:
                return default
        return cur if isinstance(cur, str) else default
    except Exception:
        return default


def _nominatim_geocode(query: str, email: Optional[str]) -> Tuple[Optional[float], Optional[float], str]:
    if not requests or not query:
        return None, None, "unavailable"
    try:
        params = {"q": query, "format": "json", "limit": 1, "addressdetails": 0}
        if email:
            params["email"] = email
        headers = {"User-Agent": "ca-admin/1.0 (+geocode)"}
        resp = requests.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers, timeout=20)
        if resp.status_code != 200:
            return None, None, f"http-{resp.status_code}"
        data = resp.json()
        if not data:
            return None, None, "noresult"
        lat = float(data[0].get("lat")) if data[0].get("lat") is not None else None
        lon = float(data[0].get("lon")) if data[0].get("lon") is not None else None
        if lat is None or lon is None:
            return None, None, "noresult"
        return lat, lon, "ok"
    except Exception:
        return None, None, "error"


def _maybe_autogeocode(row: Dict[str, Any]) -> None:
    """Fill Latitude/Longitude if missing using Nominatim and configured email."""
    lat = row.get("Latitude")
    lon = row.get("Longitude")
    if (lat not in (None, "")) and (lon not in (None, "")):
        return
    address_bits = [row.get("Address") or "", row.get("City") or "", row.get("State") or "", str(row.get("ZIP") or ""), "USA"]
    q = ", ".join([str(x).strip() for x in address_bits if str(x).strip()])
    if not q:
        return
    email = _config_value(["geocode_email"]) or _config_value(["flags", "geocode_email"]) or _config_value(["email"]) or None
    glat, glon, status = _nominatim_geocode(q, email)
    if glat is not None and glon is not None:
        row["Latitude"] = glat
        row["Longitude"] = glon























def _append_terminated_employee(employee_id: str, employee_name: Optional[str], property_name: Optional[str], terminated_on: Optional[str]) -> None:







    path = TERMINATED_LOG_PATH







    if not terminated_on:







        terminated_on = datetime.utcnow().date().isoformat()







    if path.exists():







        df = pd.read_excel(path, engine="openpyxl")







    else:







        df = pd.DataFrame(columns=["EmployeeID", "EmployeeName", "Property", "TerminatedOn"])







    df = _ensure_columns(df, ["EmployeeID", "EmployeeName", "Property", "TerminatedOn"])







    row = {







        "EmployeeID": employee_id,







        "EmployeeName": employee_name or "",







        "Property": property_name or "",







        "TerminatedOn": terminated_on,







    }







    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    _write_single_sheet(path, df)























def _search_employees_by_name(query: str, limit: int = 10) -> List[Dict[str, Any]]:







    trimmed = (query or "").strip()







    if not trimmed:







        return []







    canonical_query = trimmed.casefold()







    suggestions: List[Tuple[int, Dict[str, Any]]] = []







    with datastore.lock:







        for property_id, positions in datastore.positions_by_property.items():







            property_record = datastore.properties_payload.get(property_id) or {}







            property_name = property_record.get("property")







            for position in positions:







                employee_id = position.get("employeeId")







                employee_name = position.get("employeeName")







                if not employee_name and not employee_id:







                    continue







                job_title = position.get("jobTitle")







                haystack = " ".join(







                    part for part in [employee_name or "", employee_id or "", property_name or "", job_title or ""] if part







                )







                haystack_case = haystack.casefold()







                if canonical_query in haystack_case:







                    score = 100







                else:







                    score = fuzz.WRatio(trimmed, haystack) if haystack else 0







                    if score < 55:







                        continue







                suggestions.append(







                    (







                        score,







                        {







                            "employeeId": employee_id,







                            "employeeName": employee_name,







                            "jobTitle": job_title,







                            "property": property_name,







                            "propertyId": property_id,







                        },







                    )







                )







    suggestions.sort(key=lambda item: (-item[0], (item[1].get("employeeName") or "").casefold()))







    return [item[1] for item in suggestions[:limit]]























def _resolve_property_reference(reference: Optional[str]) -> Tuple[Optional[str], Optional[str]]:







    ref = _normalize_text(reference)







    if not ref:







        return None, None







    canonical_ref = datastore._canonical(ref)







    properties = datastore.get_properties()







    for prop in properties:







        prop_id = _clean_identifier(prop.get("propertyId"))







        prop_name = _normalize_text(prop.get("property"))







        if prop_id and datastore._canonical(prop_id) == canonical_ref:







            return prop_id, prop_name







        if prop_name and datastore._canonical(prop_name) == canonical_ref:







            return prop_id, prop_name







    return ref, ref























def _remove_property_records(property_id: Optional[str], property_name: Optional[str]) -> Tuple[int, bool]:







    canonical_id = datastore._canonical(property_id) if property_id else ""







    canonical_name = datastore._canonical(property_name) if property_name else ""







    if not canonical_id and not canonical_name:







        raise ValueError("Property name or ID is required")







    with datastore.lock:







        df = _read_single_sheet(PROPERTY_FILE_PATH)







        df = _ensure_columns(df, PROPERTY_COLUMN_ORDER)







        df = _reorder_columns(df, PROPERTY_COLUMN_ORDER)







        id_column = PROPERTY_FIELD_MAP["propertyId"]







        name_column = PROPERTY_FIELD_MAP["property"]







        mask = pd.Series([False] * len(df))







        if canonical_id:







            mask = mask | (df[id_column].fillna("").astype(str).map(datastore._canonical) == canonical_id)







        if canonical_name:







            mask = mask | (df[name_column].fillna("").astype(str).map(datastore._canonical) == canonical_name)







        if not mask.any():







            return 0, False







        columns_to_clear = [column for column in df.columns if column != id_column]







        df.loc[mask, columns_to_clear] = pd.NA







        _write_single_sheet(PROPERTY_FILE_PATH, df)















        positions_updated = False







        try:







            sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")







        except FileNotFoundError:







            sheets = None







        if sheets:







            for sheet_name in ("Positions", "Unmatched_Properties"):







                sheet_df = sheets.get(sheet_name)







                if sheet_df is None:







                    continue







                sheet_mask = pd.Series([False] * len(sheet_df))







                if canonical_id and "PropertyID" in sheet_df.columns:







                    sheet_mask = sheet_mask | (







                        sheet_df["PropertyID"].fillna("").astype(str).map(datastore._canonical) == canonical_id







                    )







                if canonical_name and "Property" in sheet_df.columns:







                    sheet_mask = sheet_mask | (







                        sheet_df["Property"].fillna("").astype(str).map(datastore._canonical) == canonical_name







                    )







                if sheet_mask.any():
                    # Drop matching rows instead of clearing columns to avoid stale positions
                    sheet_df = sheet_df.loc[~sheet_mask].copy()
                    sheets[sheet_name] = sheet_df
                    positions_updated = True
                    continue







                    if "EmployeeID" in sheet_df.columns:







                        sheet_df.loc[sheet_mask, "EmployeeID"] = ""







                    if "Employee First Name" in sheet_df.columns:







                        sheet_df.loc[sheet_mask, "Employee First Name"] = ""







                    if "Employee Last Name" in sheet_df.columns:







                        sheet_df.loc[sheet_mask, "Employee Last Name"] = ""







                    if "Property" in sheet_df.columns:







                        sheet_df.loc[sheet_mask, "Property"] = ""







                    if "PropertyID" in sheet_df.columns:







                        sheet_df.loc[sheet_mask, "PropertyID"] = ""







                    sheets[sheet_name] = sheet_df







                    positions_updated = True







            if positions_updated:







                _write_workbook(POSITIONS_FILE_PATH, sheets)



    datastore.reload()







    return int(mask.sum()), positions_updated

@app.route("/api/admin/cleanup", methods=["POST"])
def admin_cleanup():
    """One-click data cleanup: de-dupe properties by ID and harden Employees sheet."""
    guard = _require_admin()
    if guard:
        return guard

    results: Dict[str, Any] = {"ok": True}

    # Properties: enforce unique PropertyID
    try:
        dfp = _read_single_sheet(PROPERTY_FILE_PATH)
        dfp = _ensure_columns(dfp, PROPERTY_COLUMN_ORDER)
        dfp = _reorder_columns(dfp, PROPERTY_COLUMN_ORDER)
        pid_col = PROPERTY_FIELD_MAP["propertyId"]

        def _blank_prop_row(idx: int) -> bool:
            row = dfp.loc[idx]
            for col in dfp.columns:
                if col == pid_col:
                    continue
                val = row.get(col)
                if str(val).strip():
                    return False
            return True

        removed = 0
        seen: Dict[str, int] = {}
        drop_indices: List[int] = []
        for idx, val in dfp[pid_col].fillna("").astype(str).items():
            key = datastore._canonical(val)
            if not key:
                continue
            if key not in seen:
                seen[key] = idx
                continue
            keep_idx = seen[key]
            # Prefer non-blank row as keeper
            if _blank_prop_row(keep_idx) and not _blank_prop_row(idx):
                drop_indices.append(keep_idx)
                seen[key] = idx
            else:
                drop_indices.append(idx)
        if drop_indices:
            removed = len(drop_indices)
            dfp = dfp.drop(index=drop_indices).reset_index(drop=True)
            _write_single_sheet(PROPERTY_FILE_PATH, dfp)
        results["properties_removed"] = removed
    except Exception as exc:
        results["ok"] = False
        results["properties_error"] = str(exc)

    # Employees: restrict columns and collapse duplicate IDs
    try:
        dfe = _read_single_sheet(EMPLOYEE_FILE_PATH)
        dfe = _ensure_columns(dfe, EMPLOYEE_COLUMN_ORDER)
        dfe = _reorder_columns(dfe, EMPLOYEE_COLUMN_ORDER)
        # Drop any extra columns if present
        dfe = dfe[[c for c in EMPLOYEE_COLUMN_ORDER if c in dfe.columns]]
        id_col = EMPLOYEE_FIELD_MAP["employeeId"]
        drop_e: List[int] = []
        seen_e: set[str] = set()
        for idx, val in dfe[id_col].fillna("").astype(str).items():
            key = datastore._canonical(val)
            if not key:
                continue
            if key in seen_e:
                drop_e.append(idx)
            else:
                seen_e.add(key)
        if drop_e:
            dfe = dfe.drop(index=drop_e).reset_index(drop=True)
        _write_single_sheet(EMPLOYEE_FILE_PATH, dfe)
        results["employees_removed"] = len(drop_e)
    except Exception as exc:
        results["ok"] = False
        results["employees_error"] = str(exc)

    datastore.reload()
    return jsonify(results)























def _remove_employee_record(employee_id: Optional[str], terminated_on: Optional[str] = None) -> Tuple[int, Optional[Dict[str, Any]]]:







    canonical_id = datastore._canonical(employee_id) if employee_id else ""







    if not canonical_id:







        raise ValueError("Employee ID is required")







    with datastore.lock:







        df = _read_single_sheet(EMPLOYEE_FILE_PATH)







        df = _ensure_columns(df, EMPLOYEE_COLUMN_ORDER)







        df = _reorder_columns(df, EMPLOYEE_COLUMN_ORDER)







        id_column = EMPLOYEE_FIELD_MAP["employeeId"]







        mask = df[id_column].fillna("").astype(str).map(datastore._canonical) == canonical_id







        if not mask.any():







            return 0, None







        row_index = df[mask].index[0]







        record = df.loc[row_index].to_dict()







        columns_to_clear = [column for column in df.columns if column != id_column]







        df.loc[row_index, columns_to_clear] = pd.NA







        _write_single_sheet(EMPLOYEE_FILE_PATH, df)















        property_for_log = _normalize_text(record.get("Property"))







        employee_name = _normalize_text(record.get("Employee Name"))







        if not employee_name:







            employee_name = " ".join(part for part in [record.get("First Name"), record.get("Last Name")] if part) or None















        positions_updated = False







        try:







            sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")







        except FileNotFoundError:







            sheets = None







        if sheets and "Positions" in sheets:







            positions_df = sheets["Positions"]







            if "EmployeeID" in positions_df.columns:







                pos_mask = positions_df["EmployeeID"].fillna("").astype(str).map(datastore._canonical) == canonical_id







            else:







                pos_mask = pd.Series([False] * len(positions_df))







            if pos_mask.any():







                pos_row = positions_df[pos_mask].iloc[0].to_dict()







                property_for_log = property_for_log or _normalize_text(pos_row.get("Property"))







                if "EmployeeID" in positions_df.columns:







                    positions_df.loc[pos_mask, "EmployeeID"] = ""







                if "Employee First Name" in positions_df.columns:







                    positions_df.loc[pos_mask, "Employee First Name"] = ""







                if "Employee Last Name" in positions_df.columns:







                    positions_df.loc[pos_mask, "Employee Last Name"] = ""







                sheets["Positions"] = positions_df







                positions_updated = True







            if positions_updated:







                _write_workbook(POSITIONS_FILE_PATH, sheets)











    _append_terminated_employee(







        employee_id,







        employee_name,







        property_for_log,







        terminated_on,







    )







    datastore.reload()







    return 1, {







        "employeeId": employee_id,







        "employeeName": employee_name,







        "property": property_for_log,







        "terminatedOn": terminated_on or datetime.utcnow().date().isoformat(),







    }























def _perform_transfer(payload: Dict[str, Any]) -> Dict[str, Any]:







    employee_id = _normalize_text(payload.get("employeeId"))







    employee_name = _normalize_text(payload.get("employeeName"))







    to_property_ref = _normalize_text(payload.get("toProperty"))







    position_title = _normalize_text(payload.get("position"))







    entered_by = _normalize_text(payload.get("enteredBy"))







    notes = _normalize_text(payload.get("notes"))







    effective_date = _normalize_text(payload.get("effectiveDate"))







    confirm_replace = bool(payload.get("confirmReplace"))















    if not position_title:







        raise ValueError("Position is required")







    if not employee_id and not employee_name:







        raise ValueError("Employee selection is required")















    if not employee_id and employee_name:







        for record in _load_employees():







            if datastore._canonical(record.get("employeeName")) == datastore._canonical(employee_name):







                employee_id = record.get("employeeId")







                first_name = record.get("firstName")







                last_name = record.get("lastName")







                break







        else:







            raise ValueError("Unable to resolve employee from name")







    else:







        matched = next((record for record in _load_employees() if datastore._canonical(record.get("employeeId")) == datastore._canonical(employee_id)), None)







        if matched:







            first_name = matched.get("firstName")







            last_name = matched.get("lastName")







            employee_name = matched.get("employeeName") or employee_name







        else:







            first_name = None







            last_name = None















    if not employee_id:







        raise ValueError("Employee ID could not be determined")















    to_property_id, to_property_name = _resolve_property_reference(to_property_ref)







    if not to_property_id and not to_property_name:







        raise ValueError("Destination property is required")















    canonical_employee_id = datastore._canonical(employee_id)







    canonical_position = datastore._canonical(position_title)















    with datastore.lock:







        try:







            sheets = pd.read_excel(POSITIONS_FILE_PATH, sheet_name=None, engine="openpyxl")







        except FileNotFoundError as exc:







            raise ValueError("Positions workbook is missing") from exc







        positions_df = sheets.get("Positions")







        if positions_df is None:







            raise ValueError("Positions worksheet is missing")















        def _canonical_series(series: pd.Series) -> pd.Series:







            return series.fillna("").astype(str).map(datastore._canonical)















        employee_mask = _canonical_series(positions_df.get("EmployeeID", pd.Series(["" ] * len(positions_df)))) == canonical_employee_id







        if not employee_mask.any() and employee_name:







            employee_mask = _canonical_series(positions_df.get("Employee First Name", pd.Series(["" ] * len(positions_df))) + " " + positions_df.get("Employee Last Name", pd.Series(["" ] * len(positions_df)))) == datastore._canonical(employee_name)







        if not employee_mask.any():







            raise ValueError("Employee is not assigned to any position")







        matched_indexes = positions_df[employee_mask].index.tolist()
        if len(matched_indexes) > 1 and not payload.get("removeFromSourceIndexes"):
            options: List[Dict[str, Any]] = []
            for idx in matched_indexes:
                row = positions_df.loc[idx]
                options.append({
                    "index": int(idx),
                    "propertyId": _clean_identifier(row.get("PropertyID")),
                    "propertyName": _normalize_text(row.get("Property")),
                    "position": _normalize_text(row.get("Position Title")),
                })
            return {"status": "needs-selection", "positions": options}

        source_index = matched_indexes[0]







        source_row = positions_df.loc[source_index]







        from_property_id = _clean_identifier(source_row.get("PropertyID"))







        from_property_name = _normalize_text(source_row.get("Property"))







        job_title_current = _normalize_text(source_row.get("Position Title"))















        dest_mask = pd.Series([False] * len(positions_df))







        if to_property_id:







            dest_mask = dest_mask | (_canonical_series(positions_df.get("PropertyID", pd.Series(["" ] * len(positions_df)))) == datastore._canonical(to_property_id))







        if to_property_name:







            dest_mask = dest_mask | (_canonical_series(positions_df.get("Property", pd.Series(["" ] * len(positions_df)))) == datastore._canonical(to_property_name))







        if not dest_mask.any():







            raise ValueError("Destination property has no recorded positions")







        candidates = positions_df[dest_mask]







        if "Position Title" in candidates.columns:







            title_mask = _canonical_series(candidates["Position Title"]) == canonical_position







            candidates = candidates[title_mask]







        if candidates.empty:







            raise ValueError("No matching position found at destination property")















        vacancy_mask = candidates.get("EmployeeID", pd.Series(["" ] * len(candidates))).fillna("").astype(str).str.strip() == ""







        if vacancy_mask.any():







            destination_index = candidates[vacancy_mask].index[0]







            occupant_info = None







        else:







            destination_index = candidates.index[0]







            occupant_row = positions_df.loc[destination_index]







            occupant_info = {







                "employeeId": _clean_identifier(occupant_row.get("EmployeeID")),







                "employeeName": " ".join(







                    part







                    for part in [







                        _normalize_text(occupant_row.get("Employee First Name")),







                        _normalize_text(occupant_row.get("Employee Last Name")),







                    ]







                    if part







                ),







                "property": _normalize_text(occupant_row.get("Property")) or to_property_name,







                "position": _normalize_text(occupant_row.get("Position Title")) or position_title,







            }







            if not confirm_replace:







                return {







                    "status": "needs-confirmation",







                    "occupant": occupant_info,







                }















        if occupant_info:







            if "EmployeeID" in positions_df.columns:







                positions_df.at[destination_index, "EmployeeID"] = ""







            if "Employee First Name" in positions_df.columns:







                positions_df.at[destination_index, "Employee First Name"] = ""







            if "Employee Last Name" in positions_df.columns:







                positions_df.at[destination_index, "Employee Last Name"] = ""















        if "EmployeeID" in positions_df.columns:







            positions_df.at[destination_index, "EmployeeID"] = employee_id







        if "Employee First Name" in positions_df.columns:







            positions_df.at[destination_index, "Employee First Name"] = first_name or (employee_name.split(" ")[0] if employee_name else "")







        if "Employee Last Name" in positions_df.columns:







            positions_df.at[destination_index, "Employee Last Name"] = last_name or (employee_name.split(" ")[-1] if employee_name else "")







        if "Property" in positions_df.columns and to_property_name:







            positions_df.at[destination_index, "Property"] = to_property_name







        if "PropertyID" in positions_df.columns and to_property_id:







            positions_df.at[destination_index, "PropertyID"] = to_property_id







        if "Position Title" in positions_df.columns:







            positions_df.at[destination_index, "Position Title"] = position_title















        for column in ["EmployeeID", "Employee First Name", "Employee Last Name"]:







            # Gate clearing of source by removeFromSource flag
            remove_from_source = bool(payload.get("removeFromSource", True))
            if remove_from_source and column in positions_df.columns:







                positions_df.at[source_index, column] = ""

        # Optionally clear additional prior positions selected by the client
        remove_from_source = bool(payload.get("removeFromSource", True))
        if remove_from_source:
            extra_indexes = payload.get("removeFromSourceIndexes") or []
            try:
                indices = [int(i) for i in extra_indexes]
            except Exception:
                indices = []
            for idx in indices:
                if idx == source_index:
                    continue
                if 0 <= idx < len(positions_df):
                    for column in ["EmployeeID", "Employee First Name", "Employee Last Name"]:
                        if column in positions_df.columns:
                            positions_df.at[idx, column] = ""















        sheets["Positions"] = positions_df







        _write_workbook(POSITIONS_FILE_PATH, sheets)











    log_payload = {







        "employeeId": employee_id,







        "employeeName": employee_name,







        "fromPropertyId": from_property_id,







        "fromPropertyName": from_property_name,







        "toPropertyId": to_property_id,







        "toPropertyName": to_property_name,







        "notes": notes,







        "effectiveDate": effective_date,







        "enteredBy": entered_by,







    }







    _append_log_entry("transfers", log_payload)







    datastore.reload()







    return {







        "employeeId": employee_id,







        "employeeName": employee_name,







        "fromPropertyId": from_property_id,







        "fromPropertyName": from_property_name,







        "toPropertyId": to_property_id,







        "toPropertyName": to_property_name,







        "position": position_title,







        "enteredBy": entered_by,







    }







# === /ADDED FOR ADMIN ===























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













