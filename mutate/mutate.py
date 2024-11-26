import logging
import random

logger = logging.getLogger(__name__)


# decreases the speed of the lower priority UAV with a collision (from two colliding UAVs)
def dec_speed_of_lower_collider(trajectories, abilities):
    # Filter trajectories with collision set to True
    collision_trajectories = [t for t in trajectories if t.get('collision', False)]

    if len(collision_trajectories) <= 1:
        logger.error(f'[mutate fn] Not enough collisions to determine lower priority UAV: {collision_trajectories}')
        return False, f'Not enough collisions to determine lower priority UAV: {collision_trajectories}'

    # Find the trajectory with the highest uav_id (Lower priority)
    lowest_uav_id_trajectory = max(collision_trajectories, key=lambda t: t['uav_id'])  #TODO proper priority check

    # Decrease the speed by 25% (inplace)
    original_speed = lowest_uav_id_trajectory['speed']
    lowest_uav_id_trajectory['speed'] = original_speed * 0.75

    # set flags
    lowest_uav_id_trajectory['origin'] = 'mutate'  # flag the updated trajectory
    lowest_uav_id_trajectory['mutation_cases'] = "001"  # binary flag for Case 1

    # Remove the "collision" key from each trajectory in collision_trajectories
    for trajectory in collision_trajectories:
        if 'collision' in trajectory:
            del trajectory['collision']

    logger.info(
        f'[mutate fn] Decreased speed of UAV {lowest_uav_id_trajectory["uav_id"]} from {original_speed} to {lowest_uav_id_trajectory["speed"]}')

    return True, trajectories
