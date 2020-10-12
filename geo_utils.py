from geopy import distance

def get_distance(pt1: tuple, pt2: tuple):
    return distance.distance(pt1, pt2).km

def calculate_path_distance(path: list):
    result = 0.0
    for idx in range(len(path) - 1):
        result += get_distance(path[idx], path[idx+1])
    return result