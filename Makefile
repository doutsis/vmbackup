################################################################################
# vmbackup - Build, Install & Deploy
#
# Usage:
#   make package              Build .deb package (Debian/Ubuntu)
#   make install              Install from source (any distro)
#   make uninstall            Remove vmbackup (keeps backup data)
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

.PHONY: package clean deploy version install uninstall

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

install:
	@echo "=== Installing $(PKG_NAME) $(VERSION) to $(INSTALL_DIR) ==="
	@test "$$(id -u)" = "0" || { echo "Error: make install must be run as root (use sudo)"; exit 1; }

	# --- Create directory structure ---
	mkdir -p $(INSTALL_DIR)
	mkdir -p $(INSTALL_DIR)/modules
	mkdir -p $(INSTALL_DIR)/lib
	mkdir -p $(INSTALL_DIR)/transports
	mkdir -p $(INSTALL_DIR)/cloud_transports
	mkdir -p $(INSTALL_DIR)/config/template

	# --- Install files with correct permissions ---
	install -m 750 vmbackup.sh             $(INSTALL_DIR)/
	install -m 640 modules/*.sh            $(INSTALL_DIR)/modules/
	install -m 640 lib/*.sh                $(INSTALL_DIR)/lib/
	install -m 750 transports/*.sh         $(INSTALL_DIR)/transports/
	install -m 750 cloud_transports/*.sh   $(INSTALL_DIR)/cloud_transports/
	install -m 640 config/template/*       $(INSTALL_DIR)/config/template/
	install -m 644 vmbackup.md             $(INSTALL_DIR)/

	# --- Default config: copy template if no existing config ---
	@if [ ! -d "$(INSTALL_DIR)/config/default" ]; then \
		mkdir -p $(INSTALL_DIR)/config/default; \
		install -m 640 config/default/* $(INSTALL_DIR)/config/default/; \
		echo "Config: installed defaults to $(INSTALL_DIR)/config/default/"; \
	else \
		echo "Config: $(INSTALL_DIR)/config/default/ already exists, not overwritten"; \
	fi

	# --- AppArmor snippet ---
	mkdir -p /etc/apparmor.d/local/abstractions
	install -m 644 apparmor/libvirt-qemu.local /etc/apparmor.d/local/abstractions/libvirt-qemu

	# --- systemd units ---
	install -m 644 systemd/vmbackup.service /lib/systemd/system/
	install -m 644 systemd/vmbackup.timer   /lib/systemd/system/

	# --- PATH symlink ---
	ln -sf $(INSTALL_DIR)/vmbackup.sh /usr/local/bin/vmbackup

	# --- Ensure backup group exists ---
	@if ! getent group backup >/dev/null 2>&1; then \
		groupadd --system backup; \
	fi

	# --- Ownership and permissions ---
	chown -R root:backup $(INSTALL_DIR)
	chmod 750 $(INSTALL_DIR)
	mkdir -p /var/log/vmbackup /run/vmbackup
	chown root:backup /var/log/vmbackup /run/vmbackup
	chmod 750 /var/log/vmbackup /run/vmbackup

	# --- Reload AppArmor profiles if active ---
	@if command -v aa-status >/dev/null 2>&1 && aa-status --enabled 2>/dev/null; then \
		for profile in /etc/apparmor.d/libvirt/libvirt-*; do \
			case "$$profile" in *.files) continue;; esac; \
			case "$$(basename $$profile)" in TEMPLATE.qemu) continue;; esac; \
			apparmor_parser -r "$$profile" 2>/dev/null || true; \
		done; \
		echo "AppArmor: reloaded libvirt VM profiles"; \
	fi

	# --- systemd ---
	systemctl daemon-reload 2>/dev/null || true
	@if ! systemctl is-enabled vmbackup.timer >/dev/null 2>&1; then \
		systemctl enable vmbackup.timer 2>/dev/null || true; \
	fi

	@echo ""
	@echo "=== $(PKG_NAME) $(VERSION) installed ==="
	@echo ""
	@echo "  Command:       vmbackup --version"
	@echo "  Timer:         sudo systemctl start vmbackup.timer"
	@echo "  Manual run:    sudo vmbackup"
	@echo "  Config:        $(INSTALL_DIR)/config/default/"
	@echo ""

clean:
	rm -rf $(BUILD_DIR)

uninstall:
	@echo "=== Uninstalling $(PKG_NAME) from $(INSTALL_DIR) ==="
	@test "$$(id -u)" = "0" || { echo "Error: make uninstall must be run as root (use sudo)"; exit 1; }

	# --- Stop and disable systemd units ---
	systemctl stop vmbackup.timer 2>/dev/null || true
	systemctl stop vmbackup.service 2>/dev/null || true
	systemctl disable vmbackup.timer 2>/dev/null || true

	# --- Remove installed files ---
	rm -f /usr/local/bin/vmbackup
	rm -f /lib/systemd/system/vmbackup.service
	rm -f /lib/systemd/system/vmbackup.timer
	rm -f /etc/apparmor.d/local/abstractions/libvirt-qemu
	rm -rf $(INSTALL_DIR)
	rm -rf /var/log/vmbackup

	systemctl daemon-reload 2>/dev/null || true

	@echo ""
	@echo "=== $(PKG_NAME) uninstalled ==="
	@echo "Backup data was not touched."
	@echo ""
