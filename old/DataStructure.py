from dataclasses import dataclass
from typing import Union, List, Optional
import json
import base64

@dataclass
class AirflowPart:
    path: str
    payload_type: str
    decoded_payload: Union[dict, str]

    def __str__(self):
        if isinstance(self.decoded_payload, dict):
            payload_str = json.dumps(self.decoded_payload, indent=2)
        else:
            payload_str = self.decoded_payload

        return (
            f"📄 Path        : {self.path}\n"
            f"📦 Payload Type: {self.payload_type}\n"
            f"📝 Payload:\n{payload_str}\n"
            f"{'-' * 60}"
        )

@dataclass
class AirflowDefinition:
    raw: str
    parts: List[AirflowPart]

    def __str__(self):
        header = "🚀 Apache Airflow Job Definition"
        divider = "=" * len(header)
        parts_str = "\n\n".join(str(part) for part in self.parts)
        return f"{header}\n{divider}\n\n{parts_str}"
  

    @classmethod
    def parse(cls, json_str: str):
        raw = json.loads(json_str)
        parts_data = raw.get("definition", {}).get("parts", [])
        parts: List[AirflowPart] = []

        for part in parts_data:
            path = part.get("path")
            payload_b64 = part.get("payload")
            payload_type = part.get("payloadType")

            try:
                decoded_bytes = base64.b64decode(payload_b64)
                try:
                    decoded = json.loads(decoded_bytes)
                except json.JSONDecodeError:
                    decoded = decoded_bytes.decode("utf-8")
            except Exception:
                decoded = None  # or raise/log as needed

            parts.append(AirflowPart(
                path=path,
                payload_type=payload_type,
                decoded_payload=decoded
            ))

        return AirflowDefinition(parts=parts, raw=json_str)