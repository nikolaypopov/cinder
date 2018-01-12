# Copyright 2016 Nexenta Systems, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import hashlib
import os
import six

from eventlet import greenthread
from oslo_log import log as logging
from oslo_utils import units

from cinder import context
from cinder import db
from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.nexenta.ns5 import jsonrpc
from cinder.volume.drivers.nexenta import options
from cinder.volume.drivers.nexenta import utils
from cinder.volume.drivers import nfs

VERSION = '1.4.0'
LOG = logging.getLogger(__name__)


class NexentaNfsDriver(nfs.NfsDriver):
    """Executes volume driver commands on Nexenta Appliance.

    Version history:
        1.0.0 - Initial driver version.
        1.1.0 - Support for extend volume.
        1.2.0 - Added HTTPS support.
                Added use of sessions for REST calls.
                Added abandoned volumes and snapshots cleanup.
        1.3.0 - Failover support.
        1.4.0 - Migrate volume support and new NEF API calls.
    """

    driver_prefix = 'nexenta'
    volume_backend_name = 'NexentaNfsDriver'
    VERSION = VERSION

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Nexenta_CI"

    def __init__(self, *args, **kwargs):
        super(NexentaNfsDriver, self).__init__(*args, **kwargs)
        if self.configuration:
            self.configuration.append_config_values(
                options.NEXENTA_CONNECTION_OPTS)
            self.configuration.append_config_values(
                options.NEXENTA_NFS_OPTS)
            self.configuration.append_config_values(
                options.NEXENTA_DATASET_OPTS)

        self.verify_ssl = self.configuration.driver_ssl_cert_verify
        self.nfs_mount_point_base = self.configuration.nexenta_mount_point_base
        self.dataset_compression = (
            self.configuration.nexenta_dataset_compression)
        self.dataset_description = (
            self.configuration.nexenta_dataset_description)
        self.sparsed_volumes = self.configuration.nexenta_sparsed_volumes
        self.nef = None
        self.use_https = self.configuration.nexenta_use_https
        self.nef_host = self.configuration.nexenta_rest_address
        self.share = self.configuration.nas_share_path
        self.nef_port = self.configuration.nexenta_rest_port
        self.nef_user = self.configuration.nexenta_user
        self.nef_password = self.configuration.nexenta_password

    @property
    def backend_name(self):
        backend_name = None
        if self.configuration:
            backend_name = self.configuration.safe_get('volume_backend_name')
        if not backend_name:
            backend_name = self.__class__.__name__
        return backend_name

    @property
    def nas_host(self):
        return (
            self.configuration.nas_host if self.configuration.nas_host
            else self.configuration.nas_ip)

    def do_setup(self, context):
        host = self.nef_host or self.nas_host
        self.nef = jsonrpc.NexentaJSONProxy(
            host, self.nef_port, self.nef_user,
            self.nef_password, self.use_https, self.verify_ssl)

    def check_for_setup_error(self):
        """Verify that the volume for our folder exists.

        :raise: :py:exc:`LookupError`
        """
        pool_name, fs = self._get_share_datasets(self.share)
        url = 'storage/pools/%s' % (pool_name)
        self.nef.get(url)
        url = 'storage/filesystems/%s' % '%2F'.join([pool_name, fs])
        self.nef.get(url)

        path = '/'.join([pool_name, fs])
        shared = False
        response = self.nef.get('nas/nfs')
        for share in response['data']:
            if share.get('filesystem') == path:
                shared = True
                break
        if not shared:
            raise LookupError(_("Dataset %s is not shared in Nexenta "
                                "Store appliance") % path)

    def create_volume(self, volume):
        """Creates a volume.

        :param volume: volume reference
        :returns: provider_location update dict for database
        """
        self._do_create_volume(volume)
        return {'provider_location': volume['provider_location']}

    def _do_create_volume(self, volume):
        pool, fs = self._get_share_datasets(self.share)
        filesystem = '%s/%s/%s' % (pool, fs, volume['name'])
        LOG.debug('Creating filesystem on NexentaStor %s', filesystem)
        url = 'storage/filesystems'
        data = {
            'path': '/'.join([pool, fs, volume['name']]),
            'compressionMode': self.dataset_compression,
        }
        try:
            self.nef.post(url, data)
        except exception.NexentaException as e:
            if 'EEXIST' in e.args[0]:
                LOG.info('Filesystem %s already exists, using it.', filesystem)
            else:
                raise
        volume['provider_location'] = '%s:/%s/%s' % (
            self.nas_host, self.share, volume['name'])
        try:
            self._share_folder(fs, volume['name'])
            self._ensure_share_mounted('%s:/%s/%s' % (
                self.nas_host, self.share, volume['name']))

            volume_size = volume['size']
            if getattr(self.configuration,
                       self.driver_prefix + '_sparsed_volumes'):
                self._create_sparsed_file(self.local_path(volume), volume_size)
            else:
                url = 'storage/filesystems/%s' % (
                    '%2F'.join([pool, fs, volume['name']]))
                compression = self.nef.get(url).get('compressionMode')
                if compression != 'off':
                    # Disable compression, because otherwise will not use space
                    # on disk.
                    self.nef.put(url, {'compressionMode': 'off'})
                try:
                    self._create_regular_file(
                        self.local_path(volume), volume_size)
                finally:
                    if compression != 'off':
                        # Backup default compression value if it was changed.
                        self.nef.put(url, {'compressionMode': compression})

        except exception.NexentaException:
            try:
                url = 'storage/filesystems/%s' % (
                    '%2F'.join([pool, fs, volume['name']]))
                self.nef.delete(url)
            except exception.NexentaException:
                LOG.warning("Cannot destroy created folder: "
                            "%(vol)s/%(folder)s",
                            {'vol': pool, 'folder': '/'.join(
                                [fs, volume['name']])})
            raise

    def _ensure_share_unmounted(self, nfs_share, mount_path=None):
        """Ensure that NFS share is unmounted on the host.

        :param nfs_share: NFS share name
        :param mount_path: mount path on the host
        """

        num_attempts = max(1, self.configuration.nfs_mount_attempts)

        if mount_path is None:
            mount_path = self._get_mount_point_for_share(nfs_share)

        if mount_path not in self._remotefsclient._read_mounts():
            LOG.info('NFS share %(share)s already unmounted from %(path)s.', {
                     'share': nfs_share,
                     'path': mount_path})
            return

        for attempt in range(num_attempts):
            try:
                self._execute('umount', mount_path, run_as_root=True)
                LOG.debug('NFS share %(share)s was successfully unmounted '
                          'from %(path)s.', {
                              'share': nfs_share,
                              'path': mount_path})
                return
            except Exception as e:
                msg = six.text_type(e)
                if attempt == (num_attempts - 1):
                    LOG.error('Unmount failure for %(share)s after '
                              '%(count)d attempts.', {
                                  'share': nfs_share,
                                  'count': num_attempts})
                    raise exception.NfsException(msg)
                LOG.warning('Unmount attempt %(attempt)d failed: %(msg)s. '
                            'Retrying unmount %(share)s from %(path)s.', {
                                'attempt': attempt,
                                'msg': msg,
                                'share': nfs_share,
                                'path': mount_path})
                greenthread.sleep(1)

    def migrate_volume(self, ctxt, volume, host):
        """Migrate if volume and host are managed by Nexenta appliance.

        :param ctxt: context
        :param volume: a dictionary describing the volume to migrate
        :param host: a dictionary describing the host to migrate to
        """
        LOG.debug('Enter: migrate_volume: id=%(id)s, host=%(host)s',
                  {'id': volume['id'], 'host': host})

        false_ret = (False, None)

        if volume['status'] not in ('available', 'retyping'):
            LOG.warning("Volume status must be 'available' or 'retyping'."
                        " Current volume status: %s", volume['status'])
            return false_ret

        if 'capabilities' not in host:
            LOG.warning("Unsupported host. No capabilities found")
            return false_ret

        capabilities = host['capabilities']
        dst_driver_name = capabilities['location_info'].split(':')[0]
        dst_fs = capabilities['location_info'].split(':/')[1]

        if (capabilities.get('vendor_name') != 'Nexenta' or
                dst_driver_name != self.__class__.__name__ or
                capabilities['free_capacity_gb'] < volume['size']):
            return false_ret

        pool, fs = self._get_share_datasets(self.share)
        url = 'hpr/services'
        svc_name = 'cinder-migrate-%s' % volume['name']
        data = {
            'name': svc_name,
            'sourceDataset': '/'.join([pool, fs, volume['name']]),
            'destinationDataset': '/'.join([dst_fs, volume['name']]),
            'type': 'scheduled',
            'sendShareNfs': True,
        }
        nef_ips = capabilities['nef_url'].split(',')
        if capabilities['nef_url'] != self.nef_host:
            data['isSource'] = True
            data['remoteNode'] = {
                'host': nef_ips[0],
                'port': capabilities['nef_port']
            }
        try:
            self.nef.post(url, data)
        except exception.NexentaException as exc:
            if 'ENOENT' in exc.args[0] and len(nef_ips) > 1:
                data['remoteNode']['host'] = nef_ips[1]
                self.nef.post(url, data)
            else:
                raise

        url = 'hpr/services/%s/start' % svc_name
        self.nef.post(url)
        provider_location = '/'.join([
            capabilities['location_info'].strip(dst_driver_name).strip(':'),
            volume['name']])

        params = (
            '?destroySourceSnapshots=true&destroyDestinationSnapshots=true')
        in_progress = True
        url = 'hpr/services/%s' % svc_name
        timeout = 1
        while in_progress:
            state = self.nef.get(url)['state']
            if state == 'disabled':
                in_progress = False
            elif state == 'enabled':
                greenthread.sleep(timeout)
                timeout = timeout * 2
            else:
                url = 'hpr/services/%s%s' % (svc_name, params)
                self.nef.delete(url)
                return false_ret

        url = 'hpr/services/%s%s' % (svc_name, params)
        self.nef.delete(url)

        try:
            self.delete_volume(volume)
        except exception.NexentaException as exc:
            LOG.warning("Cannot delete source volume %(volume)s on "
                        "NexentaStor Appliance: %(exc)s",
                        {'volume': volume['name'], 'exc': exc})

        return True, {'provider_location': provider_location}

    def initialize_connection(self, volume, connector):
        LOG.debug('Initialize volume connection for %s', volume['name'])
        url = 'hpr/activate'
        data = {'datasetName': volume['provider_location'].split(':/')[1]}
        self.nef.post(url, data)
        data = {'export': volume['provider_location'], 'name': 'volume'}
        return {
            'driver_volume_type': self.driver_volume_type,
            'data': data,
            'mount_point_base': self.nfs_mount_point_base
        }

    def delete_volume(self, volume):
        """Deletes a logical volume.

        :param volume: volume reference
        """
        pool, fs = self._get_share_datasets(self.share)
        url = 'storage/filesystems/%s' % '%2F'.join(
            [pool, fs, volume['name']])

        field = 'originalSnapshot'
        origin = self.nef.get(url).get(field)
        url = 'storage/filesystems/%s?snapshots=true' % '%2F'.join(
            [pool, fs, volume['name']])
        try:
            self.nef.delete(url)
        except exception.NexentaException as exc:
            if 'Failed to destroy snap' in exc.kwargs['message']['message']:
                url = 'storage/snapshots?parent=%s' % '%2F'.join(
                    [pool, fs, volume['name']])
                snap_map = {}
                for snap in self.nef.get(url)['data']:
                    url = 'storage/snapshots/%s' % (
                        snap['path'].replace('/', '%2F'))
                    data = self.nef.get(url)
                    if data['clones']:
                        snap_map[data['creationTxg']] = snap['path']
                snap = snap_map[max(snap_map)]
                url = 'storage/snapshots/%s' % snap.replace('/', '%2F')
                clone = self.nef.get(url)['clones'][0]
                url = 'storage/filesystems/%s/promote' % clone.replace(
                    '/', '%2F')
                self.nef.post(url)
                url = 'storage/filesystems/%s?snapshots=true' % '%2F'.join(
                    [pool, fs, volume['name']])
                self.nef.delete(url)
            else:
                raise
        if origin and 'clone' in origin:
            url = 'storage/snapshots/%s' % origin.replace('/', '%2F')
            self.nef.delete(url)

    def extend_volume(self, volume, new_size):
        """Extend an existing volume.

        :param volume: volume reference
        :param new_size: volume new size in GB
        """
        LOG.info('Extending volume: %(id)s New size: %(size)s GB',
                 {'id': volume.id, 'size': new_size})
        self._ensure_share_mounted('%s:/%s/%s' % (
            self.nas_host, self.share, volume['name']))
        if self.sparsed_volumes:
            self._execute('truncate', '-s', '%sG' % new_size,
                          self.local_path(volume),
                          run_as_root=self._execute_as_root)
        else:
            block_size_mb = 1
            block_count = ((new_size - volume['size']) * units.Gi //
                           (block_size_mb * units.Mi))
            self._execute(
                'dd', 'if=/dev/zero',
                'seek=%d' % (volume['size'] * units.Gi / block_size_mb),
                'of=%s' % self.local_path(volume),
                'bs=%dM' % block_size_mb,
                'count=%d' % block_count,
                run_as_root=True)

    def create_snapshot(self, snapshot):
        """Creates a snapshot.

        :param snapshot: snapshot reference
        """
        volume = self._get_snapshot_volume(snapshot)
        pool, fs = self._get_share_datasets(self.share)
        url = 'storage/snapshots'

        data = {'path': '%s@%s' % ('/'.join([pool, fs, volume['name']]),
                                   snapshot['name'])}
        self.nef.post(url, data)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot.

        :param snapshot: snapshot reference
        """
        volume = self._get_snapshot_volume(snapshot)
        pool, fs = self._get_share_datasets(self.share)
        url = 'storage/snapshots/%s@%s' % ('%2F'.join(
            [pool, fs, volume['name']]), snapshot['name'])
        try:
            self.nef.delete(url)
        except exception.NexentaException:
            return

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create new volume from other's snapshot on appliance.

        :param volume: reference of volume to be created
        :param snapshot: reference of source snapshot
        """
        snapshot_vol = self._get_snapshot_volume(snapshot)
        volume['provider_location'] = snapshot_vol['provider_location']

        pool, fs = self._get_share_datasets(self.share)
        dataset_path = '%s/%s' % (pool, fs)
        fs_path = '%2F'.join([pool, fs, snapshot_vol['name']])
        url = ('storage/snapshots/%s/clone') % (
            '@'.join([fs_path, snapshot['name']]))
        path = '/'.join([pool, fs, volume['name']])
        data = {'targetPath': path}
        self.nef.post(url, data)

        try:
            self._share_folder(fs, volume['name'])
        except exception.NexentaException:
            try:
                url = ('storage/filesystems/') % (
                    '%2F'.join([pool, fs, volume['name']]))
                self.nef.delete(url)
            except exception.NexentaException:
                LOG.warning("Cannot destroy cloned filesystem: "
                            "%(vol)s/%(filesystem)s",
                            {'vol': dataset_path,
                             'filesystem': volume['name']})
            raise
        if volume['size'] > snapshot['volume_size']:
            new_size = volume['size']
            volume['size'] = snapshot['volume_size']
            self.extend_volume(volume, new_size)
            volume['size'] = new_size
        return {'provider_location': volume['provider_location']}

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume.

        :param volume: new volume reference
        :param src_vref: source volume reference
        """
        LOG.info('Creating clone of volume: %s', src_vref['id'])
        snapshot = {'volume_name': src_vref['name'],
                    'volume_id': src_vref['id'],
                    'volume_size': src_vref['size'],
                    'name': self._get_clone_snapshot_name(volume)}
        self.create_snapshot(snapshot)
        try:
            pl = self.create_volume_from_snapshot(volume, snapshot)
            return pl
        except exception.NexentaException:
            LOG.error('Volume creation failed, deleting created snapshot '
                      '%(volume_name)s@%(name)s', snapshot)
            try:
                self.delete_snapshot(snapshot)
            except (exception.NexentaException, exception.SnapshotIsBusy):
                LOG.warning('Failed to delete zfs snapshot '
                            '%(volume_name)s@%(name)s', snapshot)
            raise

    def local_path(self, volume):
        """Get volume path (mounted locally fs path) for given volume.

        :param volume: volume reference
        """
        nfs_share = volume['provider_location']
        return os.path.join(self._get_mount_point_for_share(nfs_share),
                            'volume')

    def _get_mount_point_for_share(self, nfs_share):
        """Returns path to mount point NFS share.

        :param nfs_share: example 172.18.194.100:/var/nfs
        """
        nfs_share = nfs_share.encode('utf-8')
        return os.path.join(self.configuration.nexenta_mount_point_base,
                            hashlib.md5(nfs_share).hexdigest())

    def _share_folder(self, path, filesystem):
        """Share NFS filesystem on NexentaStor Appliance.

        :param path: path to parent filesystem
        :param filesystem: filesystem that needs to be shared
        """
        pool = self.share.split('/')[0]
        LOG.debug(
            'Creating ACL for filesystem %s on Nexenta Store', filesystem)
        url = 'storage/filesystems/%s/acl' % (
            '%2F'.join([pool, path.replace('/', '%2F'), filesystem]))
        data = {
            "type": "allow",
            "principal": "everyone@",
            "permissions": [
                "list_directory",
                "read_data",
                "add_file",
                "write_data",
                "add_subdirectory",
                "append_data",
                "read_xattr",
                "write_xattr",
                "execute",
                "delete_child",
                "read_attributes",
                "write_attributes",
                "delete",
                "read_acl",
                "write_acl",
                "write_owner",
                "synchronize"
            ],
            "flags": [
                "file_inherit",
                "dir_inherit"
            ]
        }
        self.nef.post(url, data)

        LOG.debug(
            'Successfully shared filesystem %s', '/'.join(
                [path, filesystem]))

    def _get_capacity_info(self, path):
        """Calculate available space on the NFS share.

        :param path: example pool/nfs
        """
        pool, fs = self._get_share_datasets(path)
        url = 'storage/filesystems/%s' % '%2F'.join([pool, fs])
        data = self.nef.get(url)
        free = utils.str2size(data['bytesAvailable'])
        allocated = utils.str2size(data['bytesUsed'])
        total = free + allocated
        return total, free, allocated

    def _get_snapshot_volume(self, snapshot):
        ctxt = context.get_admin_context()
        return db.volume_get(ctxt, snapshot['volume_id'])

    def _get_share_datasets(self, nfs_share):
        pool_name, fs = nfs_share.split('/', 1)
        return pool_name, fs.replace('/', '%2F')

    def _get_clone_snapshot_name(self, volume):
        """Return name for snapshot that will be used to clone the volume."""
        return 'cinder-clone-snapshot-%(id)s' % volume

    def _is_clone_snapshot_name(self, snapshot):
        """Check if snapshot is created for cloning."""
        name = snapshot.split('@')[-1]
        return name.startswith('cinder-clone-snapshot-')

    def _update_volume_stats(self):
        """Retrieve stats info for NexentaStor appliance."""
        LOG.debug('Updating volume stats')
        total, free, allocated = self._get_capacity_info(self.share)
        total_space = utils.str2gib_size(total)
        free_space = utils.str2gib_size(free)
        share = ':/'.join([self.nas_host, self.share])

        location_info = '%(driver)s:%(share)s' % {
            'driver': self.__class__.__name__,
            'share': share
        }
        self._stats = {
            'vendor_name': 'Nexenta',
            'compression': self.dataset_compression,
            'description': self.dataset_description,
            'nef_url': self.nef_host,
            'nef_port': self.nef_port,
            'driver_version': self.VERSION,
            'storage_protocol': 'NFS',
            'sparsed_volumes': self.sparsed_volumes,
            'total_capacity_gb': total_space,
            'free_capacity_gb': free_space,
            'reserved_percentage': self.configuration.reserved_percentage,
            'QoS_support': False,
            'location_info': location_info,
            'volume_backend_name': self.backend_name,
            'nfs_mount_point_base': self.nfs_mount_point_base
        }

    def get_original_snapshot_url(self, zfs_object):
        return 'storage/snapshots/%s' % zfs_object.replace('/', '%2F')

    def get_delete_volume_url(self, zfs_object):
        return self.get_original_snapshot_url(zfs_object) + '?force=true'
