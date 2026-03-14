################################################################################
# vmbackup - Debian Package Build & Deploy
#
# Usage:
#   make package              Build .deb package
#   make deploy TARGET=root@host        SCP + install on target host
#   make clean                Remove build artifacts
#   make version              Show current version
#
# The version is read from VMBACKUP_VERSION in vmbackup.sh - bump it there.
################################################################################

PKG_NAME    := vmbackup
VERSION     := $(shell grep '^VMBACKUP_VERSION=' vmbackup.sh | head -1 | sed 's/.*"\(.*\)"/\1/')
ARCH        := all
INSTALL_DIR := /opt/vmbackup
BUILD_DIR   := build
PKG_DIR     := $(BUILD_DIR)/$(PKG_NAME)_$(VERSION)_$(ARCH)
DEB_FILE    := $(PKG_DIR).deb

.PHONY: package clean deploy version

version:
	@echo "$(PKG_NAME) $(VERSION)"

package: clean
	@echo "=== Building $(PKG_NAME) $(VERSION) ==="

	# --- Directory structure ---
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/modules
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/lib
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/transports
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/cloud_transports
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/config/default
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/config/template
	mkdir -p $(PKG_DIR)/DEBIAN

	# --- Main scripts (750: root + backup group, no world) ---
	install -m 750 vmbackup.sh             $(PKG_DIR)$(INSTALL_DIR)/

	# --- Modules (640: root rw, backup group read, no world) ---
	install -m 640 modules/*.sh            $(PKG_DIR)$(INSTALL_DIR)/modules/

	# --- Libraries (640: root rw, backup group read, no world) ---
	install -m 640 lib/*.sh                $(PKG_DIR)$(INSTALL_DIR)/lib/

	# --- Transports (750: root + backup group, no world) ---
	install -m 750 transports/*.sh         $(PKG_DIR)$(INSTALL_DIR)/transports/

	# --- Cloud transports (750: root + backup group, no world) ---
	install -m 750 cloud_transports/*.sh   $(PKG_DIR)$(INSTALL_DIR)/cloud_transports/

	# --- Configs: default (640: root rw, backup group read, no world) ---
	install -m 640 config/default/*        $(PKG_DIR)$(INSTALL_DIR)/config/default/

	# --- Configs: template (640: root rw, backup group read, no world) ---
	install -m 640 config/template/*       $(PKG_DIR)$(INSTALL_DIR)/config/template/

	# --- AppArmor snippet ---
	mkdir -p $(PKG_DIR)/etc/apparmor.d/local/abstractions
	install -m 644 apparmor/libvirt-qemu.local $(PKG_DIR)/etc/apparmor.d/local/abstractions/libvirt-qemu

	# --- Documentation ---
	install -m 644 vmbackup.md $(PKG_DIR)$(INSTALL_DIR)/

	# --- systemd units ---
	mkdir -p $(PKG_DIR)/lib/systemd/system
	install -m 644 systemd/vmbackup.service $(PKG_DIR)/lib/systemd/system/
	install -m 644 systemd/vmbackup.timer   $(PKG_DIR)/lib/systemd/system/

	# --- DEBIAN metadata ---
	sed 's/__VERSION__/$(VERSION)/' debian/control > $(PKG_DIR)/DEBIAN/control
	install -m 644 debian/conffiles        $(PKG_DIR)/DEBIAN/
	install -m 755 debian/postinst         $(PKG_DIR)/DEBIAN/
	install -m 755 debian/postrm           $(PKG_DIR)/DEBIAN/

	# --- Build ---
	dpkg-deb --build --root-owner-group $(PKG_DIR)

	@echo ""
	@echo "=== Package built: $(DEB_FILE) ==="
	@echo "    Size: $$(du -h $(DEB_FILE) | cut -f1)"
	@echo ""
	@echo "To deploy:  make deploy TARGET=root@host"

deploy:
	@test -n "$(TARGET)" || { echo "Usage: make deploy TARGET=user@host"; exit 1; }
	@test -f "$(DEB_FILE)" || { echo "No package found. Run 'make package' first."; exit 1; }
	@echo "=== Deploying $(PKG_NAME) $(VERSION) to $(TARGET) ==="
	scp $(DEB_FILE) $(TARGET):/tmp/$(PKG_NAME)_$(VERSION)_$(ARCH).deb
	ssh $(TARGET) "dpkg -i /tmp/$(PKG_NAME)_$(VERSION)_$(ARCH).deb && rm -f /tmp/$(PKG_NAME)_$(VERSION)_$(ARCH).deb"
	@echo ""
	@echo "=== Deployed $(PKG_NAME) $(VERSION) to $(TARGET) ==="

clean:
	rm -rf $(BUILD_DIR)
