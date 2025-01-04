# Description: This file contains the functions to create the volume related templates for the Argo workflows.
from typing import Optional, List, Dict

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

class VolumeTemplates:
    """
    A collection of utility methods for creating volume templates for Argo workflows.
    """

    @staticmethod
    def create_volume_claim_template(
        name: str,
        storage_class_name: Optional[str] = None,
        storage_size: Optional[str] = None,
        access_modes: Optional[List[str]] = None,
    ) -> PersistentVolumeClaim:
        """
        Creates a PersistentVolumeClaim template.

        Args:
            name (str): Name of the volume claim.
            storage_class_name (Optional[str]): Storage class name.
            storage_size (Optional[str]): Requested storage size (e.g., '1Gi').
            access_modes (Optional[List[str]]): List of access modes (e.g., ['ReadWriteOnce']).

        Returns:
            PersistentVolumeClaim: A volume claim object.
        """
        if not storage_size:
            raise ValueError("Storage size must be specified.")

        return PersistentVolumeClaim(
            metadata=ObjectMeta(name=name),
            spec=PersistentVolumeClaimSpec(
                access_modes=access_modes,
                storage_class_name=storage_class_name,
                resources=ResourceRequirements(
                    requests={
                        "storage": Quantity(__root__=storage_size),
                    }
                ),
            ),
        )

    @staticmethod
    def create_secret_volume(name: str, secret_name: str) -> Volume:
        """
        Creates a volume from a Kubernetes secret.

        Args:
            name (str): Name of the volume.
            secret_name (str): Name of the Kubernetes secret.

        Returns:
            Volume: A secret volume object.
        """
        if not secret_name:
            raise ValueError("Secret name must be specified.")

        return Volume(name=name, secret=SecretVolumeSource(secret_name=secret_name))

    @staticmethod
    def create_config_map_volume(
        name: str, config_map_name: str, items: List[Dict[str, str]], default_mode: int, optional: bool
    ) -> Volume:
        """
        Creates a volume from a Kubernetes ConfigMap.

        Args:
            name (str): Name of the volume.
            config_map_name (str): Name of the ConfigMap.
            items (List[Dict[str, str]]): List of key-path-mode mappings for the ConfigMap.
            default_mode (int): Default file permission mode.
            optional (bool): Whether the ConfigMap is optional.

        Returns:
            Volume: A ConfigMap volume object.
        """
        if not config_map_name:
            raise ValueError("ConfigMap name must be specified.")

        key_to_path_items = []
        for item in items:
            if "key" not in item or "path" not in item:
                raise ValueError("Each item must have a 'key' and 'path'.")
            key_to_path_items.append(
                KeyToPath(key=item["key"], path=item["path"], mode=item.get("mode"))
            )

        return Volume(
            name=name,
            config_map=ConfigMapVolumeSource(
                name=config_map_name,
                items=key_to_path_items,
                default_mode=default_mode,
                optional=optional,
            ),
        )

    @staticmethod
    def create_persistent_volume_claim(name: str, claim_name: str) -> Volume:
        """
        Creates a volume from an existing PersistentVolumeClaim.

        Args:
            name (str): Name of the volume.
            claim_name (str): Name of the PersistentVolumeClaim.

        Returns:
            Volume: A PersistentVolumeClaim volume object.
        """
        if not claim_name:
            raise ValueError("Claim name must be specified.")

        return Volume(
            name=name,
            persistent_volume_claim=PersistentVolumeClaimVolumeSource(claim_name=claim_name),
        )