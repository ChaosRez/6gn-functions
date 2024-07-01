import logging
import random

logger = logging.getLogger(__name__)


def generate_mutations(trajectories, abilities):
    new_candidates = []

    # Iterate over each uav trajectories in data
    for i, trajectory in enumerate(trajectories):
        mutated_trajectories = trajectories.copy()  # Create a copy of the trajectories
        uav_type = trajectory.get('uav_type', None)
        if uav_type is None:
            logger.error(f'[mutate fn] No uav_type key found in trajectory: {trajectory}')
            return 'No origin key found in meta'

        # Retrieve the min_speed and max_speed for the uav_type from abilities
        uav_ability = abilities.get(uav_type, {})

        # Generate a random speed in the range of min_speed and max_speed
        new_speed = random.uniform(uav_ability.get('min_speed', 0), uav_ability.get('max_speed', 0))

        # Generate a random bearing change that is less than max_bearing
        bearing_change = random.uniform(-uav_ability.get('max_bearing', 0), uav_ability.get('max_bearing', 0))

        # The trajectory with the updated speed and direction
        new_trajectory = trajectory.copy()  # Create a copy of the trajectory
        new_trajectory['origin'] = 'mutate'  # flag the updated trajectory
        new_trajectory['speed'] = new_speed
        new_trajectory['direction'] = (new_trajectory[
                                           'direction'] + bearing_change) % 360  # Ensure the direction is within [0, 360)

        # Replace the current trajectory in the mutated_trajectories list with the new trajectory
        mutated_trajectories[i] = new_trajectory

        # Add the new candidate to the list of candidates
        new_candidates.append(mutated_trajectories)
        logger.info(f'[mutate fn] added new candidate: {mutated_trajectories}')

    return new_candidates
