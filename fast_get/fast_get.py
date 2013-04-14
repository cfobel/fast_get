from __future__ import division
import re
from collections import OrderedDict
import cStringIO as StringIO
from copy import deepcopy
from datetime import datetime
from threading import Thread
import time

import numpy as np
import socket
import requests
import zmq


socket.setdefaulttimeout(0.0001)


def remove_overlaps(input_data):
    data = deepcopy(input_data)
    to_remove = set()
    for i in range(len(data) - 1):
        for k in range(i + 1, len(data)):
            if data[i][1] >= data[k][0]:
                data[i][1] = data[k][1]
                to_remove.add(k)
            else:
                break
    return [d for i, d in enumerate(data) if i not in to_remove]


def get_range_responses(url, connection_count=4, byte_count=None):
    # Determine the size of the file at the URL
    request = requests.get(url, stream=True)
    total_byte_count = int(request.headers['content-length'])

    if byte_count is None:
        byte_count = total_byte_count
    elif byte_count > total_byte_count:
        raise ValueError, ('Requested byte count (%s) exceeds available bytes '
                           '(%s).' % (byte_count, total_byte_count))

    # Split the file into as many sections as we have connections, adding any
    # remainder from uneven division to the last section.
    target_size = byte_count // connection_count
    extra_byte_count = byte_count - target_size * connection_count
    section_byte_counts = [target_size] * connection_count
    section_byte_counts[-1] += extra_byte_count
    assert(sum(section_byte_counts) == byte_count)

    # For each section, request a streaming response for the corresponding
    # bytes range of the file.  These responses can then be used to read the
    # corresponding sections of the file using either the response's `read`
    # method (allowing iterative reading), or by reading the entire section in
    # one call by accessing the `content` attribute.
    section_byte_ranges = [(sum(section_byte_counts[:i]), sum(section_byte_counts[:i + 1]) - 1) for i in range(len(section_byte_counts))]
    data = [requests.get(url, stream=True, headers={'Range': 'bytes=%s-%s' % r}) for r in section_byte_ranges]
    return byte_count, data


cre_byte_range = re.compile(r'bytes (?P<start>\d+)\s*-\s*(?P<end>\d+)\s*/\s*(?P<total>\d+)')


def get_range_info(range_str):
    data = cre_byte_range.search(range_str).groupdict()
    return OrderedDict([(k, int(data[k])) for k in ('start', 'end', 'total')])


def get_data(i, stream_response, chunk_size=(1 << 10)):
    ctx = zmq.Context.instance()
    push = zmq.Socket(ctx, zmq.PUSH)
    push.connect('inproc://queue')
    byte_count = int(stream_response.headers['content-length'])
    range_info = get_range_info(stream_response.headers['content-range'])
    content_iterator = stream_response.iter_content(chunk_size=chunk_size)
    bytes_seen = 0
    failure_count = 0

    while bytes_seen <= byte_count or failure_count > 5:
        try:
            d = content_iterator.next()
            bytes_received = len(d)
            start = range_info['start'] + bytes_seen
            end = start + bytes_received
            push.send_multipart(map(str, (start, end)) + [d])
            bytes_seen += bytes_received
            failure_count = 0
        except StopIteration:
            break
        except Exception, e:
            import traceback; traceback.print_exc()
            failure_count += 1
    print dict(bytes_seen=bytes_seen, failure_count=failure_count)
    push.close()
    del push
    del ctx


def get_ranges(data, value):
    return [i for i, d in enumerate(data) if value >= d[0] and value <= d[1]]


def fast_get(url, connection_count=4, byte_count=None, chunk_size=(1 << 10)):
    ctx = zmq.Context.instance()
    pull = zmq.Socket(ctx, zmq.PULL)
    pull.bind('inproc://queue')

    byte_count, responses = get_range_responses(url, connection_count,
                                                byte_count=byte_count)
    threads = [Thread(target=get_data, args=(i, r, chunk_size))
               for i, r in enumerate(responses)]
    for t in threads:
        t.start()

    output = StringIO.StringIO()
    ranges = []

    while True:
        try:
            start, end, data = pull.recv_multipart(flags=zmq.NOBLOCK)
            output.seek(int(start))
            ranges.append(map(int, (start, end)))
            ranges = remove_overlaps(sorted(ranges))
            output.write(data)
            if len(ranges) == 1 and ranges[0][0] == 0 and ranges[0][1] == byte_count:
                break
        except zmq.ZMQError, e:
            if e.errno == zmq.EAGAIN:
                pass
            else:
                raise
        time.sleep(0.001)
    print 'wrote sections'
    result = output.getvalue()
    output.close()
    del ctx
    return result


def main():
    byte_count = 10 << 20
    url = 'http://remote.fobel.net/pub/0cadd539238263aaca0915b3bd065ca7/The.Office.US.S09E19.HDTV.x264-LOL.mp4'

    for chunk_size in [12 << 10]:
        start = datetime.now()
        output = fast_get(url, 5, byte_count, chunk_size)
        end = datetime.now()
        print 'chunk_size:', chunk_size, (byte_count >> 10) / (end - start).total_seconds()

    start = datetime.now()
    verify_data = requests.get(url, stream=True, headers={'Range': 'bytes=0-%s'
                                                          % (byte_count -
                                                             1)}).content
    end = datetime.now()
    print (byte_count >> 10) / (end - start).total_seconds()
    assert(len(output) == len(verify_data))
    if not (output == verify_data):
        output_array = np.array([v for v in output])
        verify_array = np.array([v for v in verify_data])
        print np.where(output_array != verify_array)
        raise ValueError, 'output != verify_data'
    return output


if __name__ == '__main__':
    result = main()
