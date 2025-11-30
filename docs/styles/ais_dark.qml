<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.44.3-Solothurn" styleCategories="Symbology">
 <pipe-data-defined-properties>
  <Option type="Map">
   <Option type="QString" name="name" value=""/>
   <Option name="properties"/>
   <Option type="QString" name="type" value="collection"/>
  </Option>
 </pipe-data-defined-properties>
 <pipe>
  <provider>
   <resampling zoomedInResamplingMethod="nearestNeighbour" maxOversampling="2" enabled="false" zoomedOutResamplingMethod="nearestNeighbour"/>
  </provider>
  <rasterrenderer nodataColor="" classificationMin="0" classificationMax="100" type="singlebandpseudocolor" alphaBand="-1" band="1" opacity="1">
   <rasterTransparency/>
   <minMaxOrigin>
    <limits>None</limits>
    <extent>WholeRaster</extent>
    <statAccuracy>Estimated</statAccuracy>
    <cumulativeCutLower>0.02</cumulativeCutLower>
    <cumulativeCutUpper>0.98</cumulativeCutUpper>
    <stdDevFactor>2</stdDevFactor>
   </minMaxOrigin>
   <rastershader>
    <colorrampshader maximumValue="100" labelPrecision="4" clip="0" classificationMode="1" minimumValue="0" colorRampType="INTERPOLATED">
     <colorramp type="gradient" name="[source]">
      <Option type="Map">
       <Option type="QString" name="color1" value="10,10,10,0,rgb:0.04,0.04,0.04,0"/>
       <Option type="QString" name="color2" value="255,255,255,255,rgb:1,1,1,1"/>
       <Option type="QString" name="direction" value="ccw"/>
       <Option type="QString" name="discrete" value="0"/>
       <Option type="QString" name="rampType" value="gradient"/>
       <Option type="QString" name="spec" value="rgb"/>
       <Option type="QString" name="stops" value="0.25;40,40,60,255,rgb:0.15,0.15,0.23,1;rgb;ccw:0.5;80,80,120,255,rgb:0.31,0.31,0.47,1;rgb;ccw:0.75;160,160,200,255,rgb:0.62,0.62,0.78,1;rgb;ccw"/>
      </Option>
     </colorramp>
     <item color="#0a0a0a" label="0.0000" alpha="0" value="0"/>
     <item color="#28283c" label="25.0000" alpha="255" value="25"/>
     <item color="#505078" label="50.0000" alpha="255" value="50"/>
     <item color="#a0a0c8" label="75.0000" alpha="255" value="75"/>
     <item color="#ffffff" label="100.0000" alpha="255" value="100"/>
     <rampLegendSettings maximumLabel="" suffix="" direction="0" orientation="2" useContinuousLegend="1" prefix="" minimumLabel="">
      <numericFormat id="basic">
       <Option type="Map">
        <Option type="invalid" name="decimal_separator"/>
        <Option type="int" name="decimals" value="6"/>
        <Option type="int" name="rounding_type" value="0"/>
        <Option type="bool" name="show_plus" value="false"/>
        <Option type="bool" name="show_thousand_separator" value="true"/>
        <Option type="bool" name="show_trailing_zeros" value="false"/>
        <Option type="invalid" name="thousand_separator"/>
       </Option>
      </numericFormat>
     </rampLegendSettings>
    </colorrampshader>
   </rastershader>
  </rasterrenderer>
  <brightnesscontrast gamma="1" brightness="0" contrast="0"/>
  <huesaturation colorizeStrength="100" invertColors="0" colorizeGreen="128" colorizeOn="0" saturation="0" colorizeBlue="128" grayscaleMode="0" colorizeRed="255"/>
  <rasterresampler maxOversampling="2"/>
  <resamplingStage>resamplingFilter</resamplingStage>
 </pipe>
 <blendMode>0</blendMode>
</qgis>
