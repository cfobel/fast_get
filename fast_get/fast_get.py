from __future__ import division
import re
from collections import OrderedDict
import cStringIO as StringIO

import numpy as np
import socket
import requests
import gevent
import zmq.green as gzmq


socket.setdefaulttimeout(0.0001)


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
    return data


cre_byte_range = re.compile(r'bytes (?P<start>\d+)\s*-\s*(?P<end>\d+)\s*/\s*(?P<total>\d+)')


def get_range_info(range_str):
    data = cre_byte_range.search(range_str).groupdict()
    return OrderedDict([(k, int(data[k])) for k in ('start', 'end', 'total')])


def get_data(i, stream_response, push):
    byte_count = int(stream_response.headers['content-length'])
    range_info = get_range_info(stream_response.headers['content-range'])
    content_iterator = stream_response.iter_content(chunk_size=(8 << 10))
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
        gevent.sleep()
    print dict(bytes_seen=bytes_seen, failure_count=failure_count)


def fast_get(url, connection_count=4, byte_count=None):
    ctx = gzmq.Context.instance()
    push = gzmq.Socket(ctx, gzmq.PUSH)
    pull = gzmq.Socket(ctx, gzmq.PULL)
    push.bind('inproc://queue')
    pull.connect('inproc://queue')

    responses = get_range_responses(url, 4, byte_count=byte_count)
    tasklets = [gevent.spawn(get_data, i, r, push) for i, r in enumerate(responses)]

    tasklet_count = len(tasklets)
    joined = set()
    output = StringIO.StringIO()
    while len(joined) < tasklet_count:
        for t in tasklets:
            if t.ready() and not t in joined:
                t.join()
                joined.add(t)
        gevent.sleep(0.001)
    print 'joined greenlets'
    while True:
        try:
            start, end, data = pull.recv_multipart(flags=gzmq.NOBLOCK)
            output.seek(int(start))
            output.write(data)
            if output.tell() >= byte_count:
                break
        except gzmq.ZMQError, e:
            if e.errno == gzmq.EAGAIN:
                break
            raise
        gevent.sleep(0.01)
    print 'wrote sections'
    result = output.getvalue()
    output.close()
    push.close()
    del push
    del ctx
    return result


def main():
    byte_count = 305430
    url = 'http://remote.fobel.net/pub/0cadd539238263aaca0915b3bd065ca7/The.Office.US.S09E19.HDTV.x264-LOL.mp4'
    output = fast_get(url, 4, byte_count)
    verify_data = requests.get(url, stream=True, headers={'Range': 'bytes=0-%s'
                                                          % (byte_count -
                                                             1)}).content
    assert(len(output) == len(verify_data))
    if not (output == verify_data):
        output_array = np.array([v for v in output])
        verify_array = np.array([v for v in verify_data])
        print np.where(output_array != verify_array)
        raise ValueError, 'output != verify_data'
    return output


if __name__ == '__main__':
    result = main()
