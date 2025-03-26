from enum import Enum
from typing import Dict
from cosmos import (
    ProjectConfig,
    ProfileConfig,
    RenderConfig,
    ExecutionConfig,
    ExecutionMode,
    LoadMode
)


class DataSet(Enum):
    CENTRAL_MAPPING = 'central_mapping'
    DEALS = 'deals'
    MANUAL = 'manual'
    ROUTY = 'routy'
    SCRAPERS = 'scrapers'
    VOLUUM = 'voluum'
    VOLUUM_MAPPER = 'voluum_mapper'


class DBTConfig:
    """
    Configuration class for DBT integration in Airflow DAGs.
    Provides methods to generate configurations
    for profiles, projects, execution, and rendering.
    """

    WORKING_DIR = "/opt/airflow"
    DAGS_PATH: str = f"{WORKING_DIR}/dags"

    @classmethod
    def profile_config(cls, **kwargs) -> ProfileConfig:
        """
        Returns the DBT profile configuration.

        Args:
            **kwargs: Additional arguments for ProfileConfig.

        Returns:
            ProfileConfig: Configuration object for DBT profiles.
        """
        return ProfileConfig(
            profile_name="dbt-public-profile",
            target_name="dev",
            profiles_yml_filepath=f"{cls.DAGS_PATH}/dbt/profiles.yml",
            **kwargs
        )

    @classmethod
    def project_config(cls, **kwargs) -> ProjectConfig:
        """
        Returns the DBT project configuration.

        Args:
            **kwargs: Additional arguments for ProjectConfig.

        Returns:
            ProjectConfig: Configuration object for the DBT project.
        """
        return ProjectConfig(
            dbt_project_path=f"{cls.DAGS_PATH}/dbt",
            env_vars=cls.read_env_file(f"{cls.WORKING_DIR}/config/.env"),
            manifest_path=f"{cls.DAGS_PATH}/dbt/target/manifest.json",
            **kwargs
        )

    @classmethod
    def execution_config(cls, **kwargs) -> ExecutionConfig:
        """
        Returns the DBT execution configuration.

        Args:
            **kwargs: Additional arguments for ExecutionConfig.

        Returns:
            ExecutionConfig: Configuration object for DBT execution.
        """
        return ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            **kwargs
        )

    @classmethod
    def render_config(cls, **kwargs) -> RenderConfig:
        """
        Returns the DBT render configuration.

        Args:
            **kwargs: Additional arguments for RenderConfig.

        Returns:
            RenderConfig: Configuration object for DBT rendering.
        """
        return RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            enable_mock_profile=False,
            emit_datasets=False,
            **kwargs
        )

    @classmethod
    def read_env_file(cls, path: str) -> Dict[str, str]:
        """
        Reads environment variables from a .env file
        and returns them as a dictionary.

        Args:
            path (str): The file path to the .env file.

        Returns:
            Dict[str, str]: A dictionary of environment variables.
        """
        env_vars = {}
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                key, value = line.split('=', 1)
                env_vars[key] = value.strip()
        return env_vars
