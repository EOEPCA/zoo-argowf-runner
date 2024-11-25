# Description: This file contains the functions to create the volume related templates for the Argo workflows.
from typing import Optional

from hera.workflows.models import (
    ConfigMapVolumeSource,
    KeyToPath,
    ObjectMeta,
    PersistentVolumeClaim,
    PersistentVolumeClaimSpec,
    PersistentVolumeClaimVolumeSource,
    Quantity,
    ResourceRequirements,
    SecretVolumeSource,
    Volume,
)


def volume_claim_template(
    name: str,
    storageClassName: Optional[str] = None,
    storageSize: Optional[str] = None,
    accessMode: Optional[list[str]] = None,
) -> PersistentVolumeClaim:
    return PersistentVolumeClaim(
        metadata=ObjectMeta(name=name),
        spec=PersistentVolumeClaimSpec(
            access_modes=accessMode,
            storage_class_name=storageClassName,
            resources=ResourceRequirements(
                requests={
                    "storage": Quantity(__root__=storageSize),
                }
            ),
        ),
    )


def secret_volume(name: str, secretName: str) -> Volume:
    return Volume(name=name, secret=SecretVolumeSource(secret_name=secretName))


def config_map_volume(
    name: str, configMapName: str, items: list[dict], defaultMode: int, optional: bool
) -> Volume:
    keyToPath_items = []
    for item in items:
        keyToPath_items.append(
            KeyToPath(key=item["key"], path=item["path"], mode=item["mode"])
        )
    return Volume(
        name=name,
        config_map=ConfigMapVolumeSource(
            name=configMapName,
            items=keyToPath_items,
            default_mode=defaultMode,
            optional=optional,
        ),
    )


def persistent_volume_claim(name: str, claimName: str) -> Volume:
    return Volume(
        name=name,
        persistent_volume_claim=PersistentVolumeClaimVolumeSource(claim_name=claimName),
    )
