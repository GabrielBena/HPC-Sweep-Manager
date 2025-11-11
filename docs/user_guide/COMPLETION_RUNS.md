# Completion Runs

## Overview

Completion runs allow you to continue a previous sweep with additional or modified parameters, automatically skipping combinations that have already been completed. This is useful when you want to:

- Add new parameter values to an existing sweep
- Re-run a sweep with a subset of parameters changed
- Expand a sweep without re-running completed experiments

## How It Works

1. **Reference the previous sweep**: Specify the sweep ID you want to complete in your sweep config
2. **Automatic filtering**: The system loads completed tasks from the previous sweep and skips matching parameter combinations
3. **Same wandb group**: New runs are automatically logged to the same wandb group as the original sweep
4. **Unified tracking**: All runs (old and new) are tracked in the same sweep directory

## Usage

### Basic Example

Create a sweep config with a `complete` field:

```yaml
# sweeps/sweep_continue.yaml
defaults:
  - override hydra/launcher: basic

sweep:
  grid:
    training.learning_rate: [5e-4, 1e-3, 2e-3]  # Added 2e-3
    training.loss_type: [l4, bce]
    training.n_message_steps: [10, 20, 30]  # Added 30
    circuit: [identity, reverse, add]

complete: sweep_20251110_212023  # The sweep ID to complete
```

### Running a Completion Sweep

```bash
# Dry run to see what will be executed
hsm sweep run -c sweeps/sweep_continue.yaml -d

# Actually run the completion sweep
hsm sweep run -c sweeps/sweep_continue.yaml
```

### What Happens

1. **Loads previous sweep data** from `sweeps/outputs/sweep_20251110_212023/`
2. **Compares parameters**: Checks which combinations were already completed
3. **Filters combinations**: Only runs new or incomplete combinations
4. **Continues task numbering**: New tasks start from where the previous sweep left off (e.g., if previous sweep had tasks 1-5, new tasks start at 6)
5. **Sets wandb.group**: Automatically uses the original sweep ID as the group
6. **Saves to same directory**: Results go to the original sweep directory

### Output Example

```
Completion Run Mode Enabled
Completing sweep: sweep_20251110_212023

Sample completed task params:
  circuit: identity
  model.use_attention_mask: true
  training.learning_rate: 5e-4
  training.loss_type: l4

Sample new combination params:
  training.learning_rate: 2e-3
  training.loss_type: l4
  training.n_message_steps: 10

Continuing task numbering from task_021

Completion run mode:
  Total combinations in new config: 60
  Already completed: 20
  New combinations to run: 40

Setting wandb.group to: sweep_20251110_212023
```

## Parameter Matching Logic

The system matches parameters based on:

- **Exact value matching**: Parameters must have identical values
- **Common keys only**: Only compares parameters present in both configs
- **Ignores wandb.group**: The `wandb.group` parameter is excluded from comparison

This means you can:
- ✅ Add new parameters (they won't affect matching)
- ✅ Add new values to existing parameters
- ✅ Change the order of parameters
- ❌ Changing existing parameter values will re-run those combinations

## Benefits

1. **Time saving**: Don't re-run expensive experiments
2. **Resource efficiency**: Use compute resources only for new experiments
3. **Unified analysis**: All results in one place with consistent wandb grouping
4. **Flexible**: Easy to iterate and expand sweeps incrementally
5. **No conflicts**: Task directories are numbered sequentially, preventing overwrites

## Tips

- Always use dry run (`-d`) first to verify which combinations will run
- The completion sweep uses the same sweep ID and directory as the original
- Check the verbose output to understand why combinations are or aren't being skipped
- Make sure the original sweep's `source_mapping.yaml` file exists and is complete
- New task directories will be numbered sequentially after existing ones (e.g., if you have task_001 through task_005, new tasks start at task_006)
- Each task gets its own directory: `sweeps/outputs/SWEEP_ID/tasks/task_XXX/`

