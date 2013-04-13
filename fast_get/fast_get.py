from __future__ import division
import requests


def fast_get(url, connections=1):
    # Determine the size of the file at the URL

    # Split the file into as many sections as we have connections

    # Start a separate read co-routine for each section

    # Wait for all sections to finish, or error
    pass


def get_range_responses(url, connection_count=4, byte_count=None):
    request = requests.get(url, stream=True)
    total_byte_count = int(request.headers['content-length'])
    if byte_count is None:
        byte_count = total_byte_count
    elif byte_count > total_byte_count:
        raise ValueError, ('Requested byte count (%s) exceeds available bytes '
                           '(%s).' % (byte_count, total_byte_count))
    target_size = byte_count // connection_count
    extra_byte_count = byte_count - target_size * connection_count
    section_byte_counts = [target_size] * connection_count
    section_byte_counts[-1] += extra_byte_count
    assert(sum(section_byte_counts) == byte_count)

    section_byte_ranges = [(sum(section_byte_counts[:i]), sum(section_byte_counts[:i + 1]) - 1) for i in range(len(section_byte_counts))]
    data = [requests.get(url, stream=True, headers={'Range': 'bytes=%s-%s' % r}) for r in section_byte_ranges]
    return data


def main():
    byte_count = 30543
    url = 'http://remote.fobel.net/pub/0cadd539238263aaca0915b3bd065ca7/The.Office.US.S09E19.HDTV.x264-LOL.mp4'

    request = requests.get(url, headers={'Range': 'bytes=0-%s' % (byte_count - 1)})
    responses = get_range_responses(url, 4, byte_count=byte_count)
    assert(request.content == ''.join([r.content for r in responses]))
    return responses


if __name__ == '__main__':
    responses = main()
