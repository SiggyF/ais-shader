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
       <Option type="QString" name="color1" value="247,251,255,0,rgb:0.9686275,0.9843137,1,0"/>
       <Option type="QString" name="color2" value="8,48,107,255,rgb:0.0313725,0.1882353,0.4196078,1"/>
       <Option type="QString" name="direction" value="ccw"/>
       <Option type="QString" name="discrete" value="0"/>
       <Option type="QString" name="rampType" value="gradient"/>
       <Option type="QString" name="spec" value="rgb"/>
       <Option type="QString" name="stops" value="0.05;222,235,247,255,rgb:0.8705882,0.9215686,0.9686275,1;rgb;ccw:0.26;198,219,239,255,rgb:0.7764706,0.8588235,0.9372549,1;rgb;ccw:0.39;158,202,225,255,rgb:0.6196078,0.7921569,0.8823529,1;rgb;ccw:0.52;107,174,214,255,rgb:0.4196078,0.6823529,0.8392157,1;rgb;ccw:0.65;66,146,198,255,rgb:0.2588235,0.572549,0.7764706,1;rgb;ccw:0.78;33,113,181,255,rgb:0.1294118,0.4431373,0.7098039,1;rgb;ccw:0.9;8,81,156,255,rgb:0.0313725,0.3176471,0.6117647,1;rgb;ccw"/>
      </Option>
     </colorramp>
     <item color="#f7fbff" label="0,0000" alpha="0" value="0"/>
     <item color="#deebf7" label="5,0000" alpha="255" value="5"/>
     <item color="#c6dbef" label="26,0000" alpha="255" value="26"/>
     <item color="#9ecae1" label="39,0000" alpha="255" value="39"/>
     <item color="#6baed6" label="52,0000" alpha="255" value="52"/>
     <item color="#4292c6" label="65,0000" alpha="255" value="65"/>
     <item color="#2171b5" label="78,0000" alpha="255" value="78"/>
     <item color="#08519c" label="90,0000" alpha="255" value="90"/>
     <item color="#08306b" label="100,0000" alpha="255" value="100"/>
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
