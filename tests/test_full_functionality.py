#!/usr/bin/env python3
"""Comprehensive test of HSM functionality including job templates."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpc_sweep_manager.core.config_parser import SweepConfig
from hpc_sweep_manager.core.param_generator import ParameterGenerator
from hpc_sweep_manager.core.job_manager import JobManager
from hpc_sweep_manager.core.utils import create_sweep_id
import tempfile
import json


def test_job_templates():
    """Test job template rendering and script generation."""
    print("🔧 Testing job template functionality...")

    # Create test parameters
    test_params = [
        {"seed": 1, "model.lr": 0.01, "model.batch_size": 32},
        {"seed": 2, "model.lr": 0.001, "model.batch_size": 64},
        {"seed": 3, "model.lr": 0.1, "model.batch_size": 128},
    ]

    # Create job manager
    job_manager = JobManager.auto_detect(
        python_path="/rds/general/user/gb21/home/anaconda3/envs/ini/bin/python",
        script_path="/rds/general/user/gb21/home/PhD/INI/PVR/scripts/train.py",
        project_dir="/rds/general/user/gb21/home/PhD/INI/PVR",
    )

    print(f"✅ Created {job_manager.system_type} job manager")

    # Test single job template rendering
    try:
        template = job_manager.jinja_env.get_template("sweep_single.sh.j2")
        script_content = template.render(
            job_name="test_job_001",
            walltime="01:00:00",
            resources="select=1:ncpus=2:mem=8gb",
            sweep_dir="/tmp/test_sweep",
            sweep_id="test_sweep_123",
            python_path=job_manager.python_path,
            script_path=job_manager.script_path,
            project_dir=job_manager.project_dir,
            params=test_params[0],
            parameters="seed=1 model.lr=0.01 model.batch_size=32",
            wandb_group="test_group",
        )
        print("✅ Single job template rendered successfully")

        # Check that the script contains expected elements
        assert "#PBS -N test_job_001" in script_content
        assert "seed=1" in script_content
        assert "model.lr=0.01" in script_content
        print("✅ Single job script validation passed")

    except Exception as e:
        print(f"❌ Single job template failed: {e}")
        return False

    # Test array job template rendering
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            sweep_dir = Path(temp_dir)

            # Create parameter combinations file like the job manager would
            params_file = sweep_dir / "parameter_combinations.json"
            indexed_combinations = [
                {"index": i + 1, "params": params}
                for i, params in enumerate(test_params)
            ]

            with open(params_file, "w") as f:
                json.dump(indexed_combinations, f, indent=2)

            template = job_manager.jinja_env.get_template("sweep_array.sh.j2")
            script_content = template.render(
                sweep_id="test_sweep_123",
                num_jobs=len(test_params),
                walltime="01:00:00",
                resources="select=1:ncpus=2:mem=8gb",
                sweep_dir=str(sweep_dir),
                python_path=job_manager.python_path,
                script_path=job_manager.script_path,
                project_dir=job_manager.project_dir,
                wandb_group="test_group",
            )

            print("✅ Array job template rendered successfully")

            # Check that the script contains expected elements
            assert "#PBS -N test_sweep_123" in script_content
            assert "#PBS -J 1-3" in script_content
            assert "$PBS_ARRAY_INDEX" in script_content
            print("✅ Array job script validation passed")

    except Exception as e:
        print(f"❌ Array job template failed: {e}")
        return False

    return True


def test_sweep_integration():
    """Test complete sweep workflow with templates."""
    print("\n🚀 Testing complete sweep integration...")

    # Load test config
    config = SweepConfig.from_yaml("tests/test_sweep.yaml")
    generator = ParameterGenerator(config)
    combinations = generator.generate_combinations(max_runs=3)

    print(f"✅ Generated {len(combinations)} parameter combinations")

    # Create job manager
    job_manager = JobManager.auto_detect(
        python_path="/rds/general/user/gb21/home/anaconda3/envs/ini/bin/python",
        script_path="/rds/general/user/gb21/home/PhD/INI/PVR/scripts/train.py",
        project_dir="/rds/general/user/gb21/home/PhD/INI/PVR",
    )

    # Test template generation without actual submission
    sweep_id = create_sweep_id()
    print(f"✅ Created sweep ID: {sweep_id}")

    with tempfile.TemporaryDirectory() as temp_dir:
        sweep_dir = Path(temp_dir)

        # Test single job script generation
        for i, params in enumerate(combinations[:2]):  # Just test first 2
            job_name = f"{sweep_id}_job_{i + 1:03d}"

            template = job_manager.jinja_env.get_template("sweep_single.sh.j2")
            script_content = template.render(
                job_name=job_name,
                walltime="01:00:00",
                resources="select=1:ncpus=2:mem=8gb",
                sweep_dir=str(sweep_dir),
                sweep_id=sweep_id,
                python_path=job_manager.python_path,
                script_path=job_manager.script_path,
                project_dir=job_manager.project_dir,
                params=params,
                parameters=job_manager._params_to_string(params),
                wandb_group="test_integration",
            )

            # Write script to verify it's valid
            script_path = sweep_dir / f"{job_name}.pbs"
            with open(script_path, "w") as f:
                f.write(script_content)

            print(f"✅ Generated script for job {i + 1}: {script_path.name}")

        # Test array job script generation
        params_file = sweep_dir / "parameter_combinations.json"
        indexed_combinations = [
            {"index": i + 1, "params": params} for i, params in enumerate(combinations)
        ]

        with open(params_file, "w") as f:
            json.dump(indexed_combinations, f, indent=2)

        template = job_manager.jinja_env.get_template("sweep_array.sh.j2")
        script_content = template.render(
            sweep_id=sweep_id,
            num_jobs=len(combinations),
            walltime="01:00:00",
            resources="select=1:ncpus=2:mem=8gb",
            sweep_dir=str(sweep_dir),
            python_path=job_manager.python_path,
            script_path=job_manager.script_path,
            project_dir=job_manager.project_dir,
            wandb_group="test_integration",
        )

        array_script_path = sweep_dir / f"{sweep_id}_array.pbs"
        with open(array_script_path, "w") as f:
            f.write(script_content)

        print(f"✅ Generated array job script: {array_script_path.name}")

    print("✅ Complete sweep integration test passed!")
    return True


if __name__ == "__main__":
    print("HSM Comprehensive Functionality Test")
    print("=" * 50)

    success = True
    success &= test_job_templates()
    success &= test_sweep_integration()

    if success:
        print("\n🎉 All comprehensive tests passed! HSM is ready to use.")
        print("\nNext steps:")
        print("1. Install the package: pip install -e .")
        print("2. Run: hsm --help")
        print("3. Initialize a project: hsm init")
        print("4. Configure sweeps: hsm configure")
        print("5. Run sweeps: hsm sweep --dry-run")
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
        sys.exit(1)
