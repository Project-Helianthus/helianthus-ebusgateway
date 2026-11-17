## Start tesla-observe before the charger gets power

The activation observation page now tells the operator to start `tesla-observe` before the charger gets power, with one repeat for each separately sequenced startup. A later repeat opens a new window after the previous source closes and does not observe the same power-up. The tool sends nothing and does not expect the confirmation. The window-bytes flag default stays 64. A sufficient capture count is not established and must be measured. The gateway binary is unchanged.
